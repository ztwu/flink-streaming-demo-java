package com.ztwu.bigdata.demo.join;

import com.alibaba.fastjson.JSONObject;
import com.ztwu.bigdata.demo.domain.People;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JoinByEventTime {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env;
		env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 10 * 1000));
		String checkpointPath = "file:///Users/kkk/checkpoints/cpk/ttttt6666";

		//重启策略
		//状态checkpoint保存
		StateBackend fsStateBackend = new FsStateBackend(checkpointPath);
		env.setStateBackend(fsStateBackend);
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
		env.enableCheckpointing(60 * 1000).getCheckpointConfig().enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		DataStream<String> nameText = env.socketTextStream("127.0.0.1", 9999);
		DataStream<People> nameStream = nameText.flatMap(new FlatMapFunction<String, People>() {
			@Override
			public void flatMap(String s, Collector<People> collector) throws Exception {
				System.out.println("name:" + s);
				String[] s1 = s.split("\\|");
				if (s1.length >= 4) {
					collector.collect(new People(s1[0], System.currentTimeMillis(), s1[2], s1[3]));
				}
			}
		}).assignTimestampsAndWatermarks(WatermarkStrategy.<People>forBoundedOutOfOrderness(Duration.ofSeconds(3))
				.withTimestampAssigner(new SerializableTimestampAssigner<People>() {
					@Override
					public long extractTimestamp(People people, long recordTimestamp) {
						return people.getEventTime();
					}
				})
		);

//				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<People>(Time.seconds(1)) {
//			@Override
//			public long extractTimestamp(People people) {
//				return people.getEventTime();
//			}
//		});

		DataStream<String> ageText = env.socketTextStream("127.0.0.1", 9998);
		DataStream<People> ageStream = ageText.flatMap(new FlatMapFunction<String, People>() {
			@Override
			public void flatMap(String s, Collector<People> collector) throws Exception {
				System.out.println("age:" + s);
				String[] s1 = s.split("\\|");
				if (s1.length >= 4) {
					// 0|1602951665626|1|1
					collector.collect(new People(s1[0], System.currentTimeMillis(), s1[2], s1[3]));
				}
			}
		}).assignTimestampsAndWatermarks(WatermarkStrategy.<People>forBoundedOutOfOrderness(Duration.ofSeconds(3))
				.withTimestampAssigner(new SerializableTimestampAssigner<People>() {
					@Override
					public long extractTimestamp(People people, long recordTimestamp) {
						return people.getEventTime();
					}
				})
		);

//				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<People>(Time.seconds(1)) {
//			@Override
//			public long extractTimestamp(People people) {
//				return people.getEventTime();
//			}
//		});

		DataStream<People> coStream = nameStream.coGroup(ageStream)
				.where(new KeySelector<People, String>() {
					@Override
					public String getKey(People people) throws Exception {
						return people.getId();
					}
				})
				.equalTo(new KeySelector<People, String>() {
					@Override
					public String getKey(People people) throws Exception {
						return people.getId();
					}
				}).window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.apply(new CoGroupFunction<People, People, People>() {
					@Override
					public void coGroup(Iterable<People> nameIterable, Iterable<People> ageIterable, Collector<People> collector) throws Exception {
						System.out.println("nameIterable:" + JSONObject.toJSONString(nameIterable));
						System.out.println("ageIterable:" + JSONObject.toJSONString(ageIterable));
						Map<String, People> tempMap = new HashMap<>();
						ageIterable.forEach(people -> tempMap.put(people.getId(), people));

						Iterator<People> iterator = nameIterable.iterator();
						while (iterator.hasNext()) {
							People people = iterator.next();
							if (tempMap.containsKey(people.getId())) {
								people.setAge(tempMap.get(people.getId()).getAge());
							}
							collector.collect(people);
						}
					}
				});

		coStream.addSink(new SinkFunction<People>() {
			@Override
			public void invoke(People value, Context context) throws Exception {
				System.out.println("addSink:" + JSONObject.toJSONString(value));
			}
		}).setParallelism(1);
		// execute program
		env.execute("Java from SocketTextStream Example");
	}
}