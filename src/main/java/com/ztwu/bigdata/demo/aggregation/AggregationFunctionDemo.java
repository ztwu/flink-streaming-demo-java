package com.ztwu.bigdata.demo.aggregation;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggregationFunctionDemo {
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(1);

		DataStream<Tuple2<Long, String>> inputStream = env.addSource(new RichSourceFunction<Tuple2<Long, String>>() {

			private static final long serialVersionUID = 1L;
			boolean flag = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
				while (flag) {
					Tuple2<Long, String> row = new Tuple2<Long, String>(1L, "b");
					ctx.collect(row);
					Thread.sleep(100);
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

		inputStream
				.keyBy((KeySelector<Tuple2<Long, String>, String>) value -> value.f1)
//				.keyBy(new KeySelector<Tuple2<Long, String>, String>() {
//			@Override
//			public String getKey(Tuple2<Long, String> rowData) throws Exception {
//				scala语法 tuple2 ==> ._2
//				return rowData._2;
//			}
//		})
//				.window(SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(2)))
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//				.timeWindow(Time.seconds(2))
				.sum(0)
				.print();
		//ReduceFunction 和 AggregateFunction 都是基于中间状态实现增量计算的窗口函数，
//				.aggregate(new AggregateFunction<Tuple2<Long, String>, Long, Long>() {
//
//					@Override
//					public Long createAccumulator() {
//						/*访问量初始化为0*/
//						return 0L;
//					}
//
//					@Override
//					public Long add(Tuple2<Long, String> value, Long accumulator) {
//						/*访问量直接+1 即可*/
//						return accumulator+1;
//					}
//
//					@Override
//					public Long getResult(Long accumulator) {
//						return accumulator;
//					}
//
//					/*合并两个统计量*/
//					@Override
//					public Long merge(Long a, Long b) {
//						return a+b;
//					}
//
//				}).print();

		env.execute("test");

	}
}
