package com.ztwu.bigdata.demo.process;

import com.alibaba.fastjson.JSON;
import com.ztwu.bigdata.demo.domain.ItemViewCount;
import com.ztwu.bigdata.demo.domain.MyBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 元素到达窗口时增量聚合，
 * 当窗口关闭时对增量聚合的结果用
 * ProcessWindowFunction再进行全量聚合。
 */
public class AggreationAndWindowFunctionDemo {

	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(60000);
		env.setParallelism(1);
		//json字符串
		DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<MyBehavior> behaviorDataStream = lines.process(new ProcessFunction<String, MyBehavior>() {
			@Override
			public void processElement(String value, Context ctx, Collector<MyBehavior> out) throws Exception {
				try {
					MyBehavior behavior = JSON.parseObject(value, MyBehavior.class);
					//输出
					out.collect(behavior);
				} catch (Exception e) {
					//e.printStackTrace();
					//TODO 记录出现异常的数据
				}
			}
		});

		//提取EventTime生成WaterMark
		SingleOutputStreamOperator<MyBehavior> behaviorDataStreamWithWaterMark = behaviorDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyBehavior>(Time.seconds(0)) {
			@Override
			public long extractTimestamp(MyBehavior element) {
				return element.getTimestamp();
			}
		});

		//按照指定的字段进行分组
		KeyedStream<MyBehavior, Tuple> keyed = behaviorDataStreamWithWaterMark.keyBy("itemId", "type");

		//窗口长度为10分组，一分钟滑动一次
		WindowedStream<MyBehavior, Tuple, TimeWindow> window = keyed.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));


		//SingleOutputStreamOperator<MyBehavior> counts = window.sum("counts");
		//自定义窗口聚合函数
		SingleOutputStreamOperator<ItemViewCount> aggDataStream = window.aggregate(new MyWindowAggFunction(), new MyWindowFunction());

		//按照窗口的start、end进行分组，将窗口相同的数据进行排序
		aggDataStream.keyBy("type", "windowStart", "windowEnd")
				.process(new KeyedProcessFunction<Tuple, ItemViewCount, List<ItemViewCount>>() {

					private transient ValueState<List<ItemViewCount>> valueState;

					@Override
					public void open(Configuration parameters) throws Exception {
						ValueStateDescriptor<List<ItemViewCount>> stateDescriptor = new ValueStateDescriptor<List<ItemViewCount>>(
								"list-state",
								TypeInformation.of(new TypeHint<List<ItemViewCount>>() {})
						);

						valueState = getRuntimeContext().getState(stateDescriptor);
					}

					@Override
					public void processElement(ItemViewCount value, Context ctx, Collector<List<ItemViewCount>> out) throws Exception {

						//将数据添加到State中缓存
						List<ItemViewCount> buffer = valueState.value();
						if(buffer == null) {
							buffer = new ArrayList<>();
						}
						buffer.add(value);
						valueState.update(buffer);
						//注册定时器
						ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
					}

					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<ItemViewCount>> out) throws Exception {

						//将ValueState中的数据取出来
						List<ItemViewCount> buffer = valueState.value();
						//按照次数降序排序
						buffer.sort(new Comparator<ItemViewCount>() {
							@Override
							public int compare(ItemViewCount o1, ItemViewCount o2) {
								return -(int)(o1.getViewCount() - o2.getViewCount());
							}
						});
						//清空State
						valueState.update(null);
						out.collect(buffer);
					}
				}).print(); //打印结果


		env.execute();

	}


	//三个泛型：
	//第一个：输入的数据类型
	//第二个：计数/累加器的类型
	//第三个：输出的数据类型
	public static class MyWindowAggFunction implements AggregateFunction<MyBehavior, Long, Long> {

		//初始化一个计数器
		@Override
		public Long createAccumulator() {
			return 0L;
		}

		//每输入一条数据就调用一次add方法
		@Override
		public Long add(MyBehavior value, Long accumulator) {
			return accumulator + value.getCounts();
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		//只针对SessionWindow有效，对应滚动窗口、滑动窗口不会调用此方法
		@Override
		public Long merge(Long a, Long b) {
			return null;
		}
	}

	//传入4个泛型
	//第一个：输入的数据类型（Long类型的次数）
	//第二个：输出的数据类型（ItemViewCount）
	//第三个：分组的key(分组的字段)
	//第四个：窗口对象（起始时间、结束时间）
	public static class MyWindowFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
			//输入的Key
			String itemId = tuple.getField(0);
			String type = tuple.getField(1);
			//窗口的起始时间
			long start = window.getStart();
			//窗口结束时间
			long end = window.getEnd();
			//窗口集合的结果
			Long count = input.iterator().next();
			//输出数据
			out.collect(new ItemViewCount(itemId, type, start, end, count));
		}
	}
}
