package com.ztwu.bigdata.demo.join;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * created with idea
 * user:ztwu
 * date:2021/5/16
 * description
 */
public class ConnectJoinDeno {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定是EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);

        //主流，用户流, 格式为：user_name、city_id、ts
        List<Entity1> list1 = new ArrayList<>();
        list1.add(new Entity1("user1", 1001, 1L));
        list1.add(new Entity1("user1", 1001, 10L));
        list1.add(new Entity1("user2", 1002, 5L));
        list1.add(new Entity1("user2", 1002, 15L));
        DataStream<Entity1> stream1 = env.fromCollection(list1)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Entity1>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Entity1 element) {
                                return element.getMytime();
                            }
                        }
                ).keyBy(new KeySelector<Entity1, Integer>() {
                    @Override
                    public Integer getKey(Entity1 value) throws Exception {
                        return value.getCityId();
                    }
                });

        //定义城市流,格式为：city_id、city_name、ts
        List<Entity2> list2 = new ArrayList<>();
        list2.add(new Entity2(1001, "beijing", 1L));
        list2.add(new Entity2(1001, "beijing2", 10L));
        list2.add(new Entity2(1002, "shanghai", 10L));
        list2.add(new Entity2(1002, "shanghai2", 5L));

        DataStream<Entity2> stream2 = env.fromCollection(list2)
                .assignTimestampsAndWatermarks(
                        //指定水位线、时间戳
                        new BoundedOutOfOrdernessTimestampExtractor<Entity2>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(Entity2 element) {
                                return element.getCityTime();
                            }
                        }).keyBy(new KeySelector<Entity2, Integer>() {
                    @Override
                    public Integer getKey(Entity2 value) throws Exception {
                        return value.getCityId();
                    }
                });

        stream1.connect(stream2).process(
                new CoProcessFunction<Entity1, Entity2, Tuple2<Entity1, Entity2>>() {
            // 流1的状态
            ValueState<Entity1> state1;
            // 流2的状态
            ValueState<Entity2> state2;
            // 定义一个用于删除定时器的状态
            ValueState<Long> timeState;

            // 定义两个侧切流的outputTag
            OutputTag<Entity1> outputTag1 = new OutputTag("stream1", TypeInformation.of(Entity1.class));
            OutputTag<Entity2> outputTag2 = new OutputTag("stream2", TypeInformation.of(Entity2.class));

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态
                state1 = getRuntimeContext().getState(new ValueStateDescriptor<Entity1>("state1", Entity1.class));
                state2 = getRuntimeContext().getState(new ValueStateDescriptor<Entity2>("state2", Entity2.class));
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("timeState", Long.class));
            }

            // 流1的处理逻辑
            @Override
            public void processElement1(Entity1 value, Context ctx,
                                        Collector<Tuple2<Entity1, Entity2>> out) throws Exception {
                Entity2 value2 = state2.value();
                // 流2不为空表示流2先来了，直接将两个流拼接发到下游
                if (value2 != null) {
                    out.collect(Tuple2.of(value, value2));
                    // 清空流2对用的state信息
                    state2.clear();
                    // 流2来了就可以删除定时器了，并把定时器的状态清除
                    ctx.timerService().deleteEventTimeTimer(timeState.value());
                    timeState.clear();
                } else {
                    // 流2还没来，将流1放入state1中，
                    state1.update(value);
                    // 并注册一个1分钟的定时器，流1中的 eventTime + 60s
                    long time = value.getMytime() + 60000;
                    timeState.update(time);
                    ctx.timerService().registerEventTimeTimer(time);
                }
            }

            // 流2的处理逻辑与流1的处理逻辑类似
            @Override
            public void processElement2(Entity2 value, Context ctx, Collector<Tuple2<Entity1, Entity2>> out) throws Exception {
                Entity1 value1 = state1.value();
                if (value1 != null) {
                    out.collect(Tuple2.of(value1,value));
                    state1.clear();
                    ctx.timerService().deleteEventTimeTimer(timeState.value());
                    timeState.clear();
                } else {
                    state2.update(value);
                    long time = value.getCityTime()+ 60000;
                    timeState.update(time);
                    ctx.timerService().registerEventTimeTimer(time);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Entity1, Entity2>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                // 定时器触发了，即1分钟内没有收到两个流
                // 流1不为空，则将流1侧切输出
                if (state1.value() != null) {
                    ctx.output(outputTag1, state1.value());
                }

                // 流2不为空，则将流2侧切输出
                if (state2.value() != null) {
                    ctx.output(outputTag2, state2.value());
                }

                state1.clear();
                state2.clear();
            }
        }).print();

        env.execute("connect");
    }
}
