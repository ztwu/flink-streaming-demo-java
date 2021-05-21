package com.ztwu.bigdata.demo.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSource {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fallBackRestart());

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		properties.setProperty("auto.offset.reset", "latest");
		properties.put("enable.auto.commit", "true");
		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer<>("topic",
						new SimpleStringSchema(), properties));
		stream.print();
		env.execute("");

	}
}
