package com.ztwu.bigdata.demo.process;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class ProcessFunctionWithTimerDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> lines = env.socketTextStream("feng05", 8888);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 得到watermark，并没有对原始数据进行处理
		SingleOutputStreamOperator<String> lineWithWaterMark = lines
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
			@Override
			public long extractTimestamp(String element) {
				return Long.parseLong(element.split(",")[0]);
			}
		});

		// 处理数据，获取指定字段
		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String value) throws Exception {
				String[] fields = value.split(",");
				return Tuple2.of(fields[1], 1);
			}
		});
		//调用keyBy进行分组
		KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
		// 没有划分窗口，直接调用底层的process方法
		keyed.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
			private transient ListState<Tuple2<String, Integer>> bufferState;
			// 定义状态描述器
			@Override
			public void open(Configuration parameters) throws Exception {
				ListStateDescriptor<Tuple2<String, Integer>> listStateDescriptor = new ListStateDescriptor<>(
						"list-state",
						TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
				);
				bufferState = getRuntimeContext().getListState(listStateDescriptor);
			}
			// 不划分窗口的话，该方法是来一条数据处理一条数据，这样输出端的压力会很大
			@Override
			public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
				//out.collect(value);
				bufferState.add(value);
				//获取当前的event time
				Long timestamp = ctx.timestamp();
				System.out.println("current event time is : " + timestamp);

				//注册定时器，如果注册的是EventTime类型的定时器，当WaterMark大于等于注册定时器的实际，就会触发onTimer方法
				ctx.timerService().registerEventTimeTimer(timestamp+10000);
			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
				Iterable<Tuple2<String, Integer>> iterable = bufferState.get();
				for (Tuple2<String, Integer> tp : iterable) {
					out.collect(tp);

				}
			}
		}).print();

		env.execute();
	}
}