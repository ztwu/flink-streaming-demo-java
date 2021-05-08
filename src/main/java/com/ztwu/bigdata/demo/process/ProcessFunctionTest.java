package com.ztwu.bigdata.demo.process;

import com.google.gson.Gson;
import com.ztwu.bigdata.demo.domain.UserAction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ProcessFunctionTest {
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 基于Event Time计算
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		Properties p = new Properties();
		p.setProperty("bootstrap.servers", "localhost:9092");
		p.setProperty("group.id", "tsdp-flink-group");
		p.setProperty("auto.offset.reset", "latest");
		p.setProperty("enable.auto.commit", "true");


		DataStreamSource<String> ds = env.addSource(new FlinkKafkaConsumer<String>("user_view_log", new SimpleStringSchema(), p));
		ds.print();

		ds.map(new MapFunction<String, UserAction>() {
				@Override
				public UserAction map(String value) throws Exception {
					return new Gson().fromJson(value, UserAction.class);
				}
			})
			// 生成水印
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserAction>() {
				@Override
				public long extractAscendingTimestamp(UserAction element) {
					try {
						return element.getUserActionTime();
					} catch (Exception e) {
						e.printStackTrace();
					}
					return 0;
				}
			})
			.keyBy(new KeySelector<UserAction, Integer>() {
				@Override
				public Integer getKey(UserAction value) throws Exception {
					return value.getUserId();
				}
			})
			// CountWithTimeoutEventTimeFunction：注册的是EventTime注册器
			// CountWithTimeoutProcessingTimeFunction：注册的是ProcessingTime注册器
			.process(new CountWithTimeoutProcessingTimeFunction())//使用process方法计算
			.print();

		env.execute("ProcessFunctionTest");
	}

}
