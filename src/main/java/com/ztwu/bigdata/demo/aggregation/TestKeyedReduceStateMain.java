package com.ztwu.bigdata.demo.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class TestKeyedReduceStateMain {
	public static void main(String[] args) throws  Exception{
		//获取执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		//StreamExecutionEnvironment.getExecutionEnvironment();
		//设置并行度
		env.setParallelism(16);
		//获取数据源
		DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
				env.fromElements(
						Tuple2.of(1L, 3L),
						Tuple2.of(1L, 7L),
						Tuple2.of(2L, 4L),
						Tuple2.of(1L, 5L),
						Tuple2.of(2L, 2L),
						Tuple2.of(2L, 6L));


		// 输出：
		//(1,5.0)
		//(2,4.0)
		dataStreamSource
				.keyBy(0)
				.flatMap(new CountAverageWithReduceState())
				.print();


		env.execute("TestStatefulApi");
	}

}