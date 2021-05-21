package com.ztwu.bigdata.demo.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaSink {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fallBackRestart());

		List<Tuple3<String, Integer, Long>> list1 = new ArrayList<>();
		list1.add(new Tuple3<>("user1", 1001, 1L));
		list1.add(new Tuple3<>("user1", 1001, 10L));
		list1.add(new Tuple3<>("user2", 1002, 5L));
		list1.add(new Tuple3<>("user2", 1002, 15L));
		DataStream<Tuple3<String, Integer, Long>> textStream = env.fromCollection(list1);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		textStream.map(new MapFunction<Tuple3<String, Integer, Long>, String>() {
			@Override
			public String map(Tuple3<String, Integer, Long> value) throws Exception {
				return value.f0;
			}
		}).addSink(new FlinkKafkaProducer<String>("topic",
						new SimpleStringSchema(), properties));
		env.execute("");

	}
}
