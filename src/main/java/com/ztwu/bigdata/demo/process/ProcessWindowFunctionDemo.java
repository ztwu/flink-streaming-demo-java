package com.ztwu.bigdata.demo.process;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class ProcessWindowFunctionDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<String, String>> map = env.socketTextStream("192.168.206.219", 9000)
			.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

				long currentTimeStamp = 0;
				long maxDelayAllowed = 0;//延迟为0
				long currentWaterMark;

				@Override
				public long extractTimestamp(String element, long previousElementTimestamp) {
					String[] split = element.split(",");
					long l = Long.parseLong(split[1]);
					currentTimeStamp = Math.max(l, currentTimeStamp);
					System.out.println("Key:" + split[0] + ",EventTime:" + l + ",水位线:" + currentWaterMark);
					return currentTimeStamp ;
				}

				@Override
				public Watermark getCurrentWatermark() {
					currentWaterMark = currentTimeStamp - maxDelayAllowed;
					return new Watermark(currentWaterMark);
				}
			})
			.map(new MapFunction<String, Tuple2<String, String>>() {
				@Override
				public Tuple2<String, String> map(String s) throws Exception {
					return new Tuple2<String, String>(s.split(",")[0], s.split(",")[1]);
				}
			});

		map.keyBy(0)
				.timeWindow(Time.seconds(5))
				.process(new ProcessWindowFunction<Tuple2<String, String>, Object, Tuple, TimeWindow>() {
					@Override
					public void process(Tuple tuple, Context context,
										Iterable<Tuple2<String, String>> elements,
										Collector<Object> out) throws Exception {
						System.out.println("fsd");
					}

				});

		env.execute();

	}
}
