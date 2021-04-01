package com.ztwu.bigdata.demo.aggregation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import scala.Tuple2;

public class ReduceFunctionDemo {
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
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

		inputStream.keyBy(new KeySelector<Tuple2<Long, String>, String>() {
			@Override
			public String getKey(Tuple2<Long, String> rowData) throws Exception {
				return rowData._2;
			}
		}).reduce(new ReduceFunction<Tuple2<Long, String>>() {
			@Override
			public Tuple2<Long, String> reduce(Tuple2<Long, String> t, Tuple2<Long, String> t1) throws Exception {
				return new Tuple2<Long,String>(t._1+t1._1, t._2);
			}
		}).print();

		env.execute("test");

	}
}
