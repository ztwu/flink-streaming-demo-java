package com.ztwu.bigdata.demo.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class MyRichSourceFunction {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(1);

		DataStream<RowData> inputStream = env.addSource(new RichSourceFunction<RowData>() {

			private static final long serialVersionUID = 1L;
			boolean flag = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void run(SourceContext<RowData> ctx) throws Exception {
				while (flag) {
					GenericRowData row = new GenericRowData(2);
					row.setField(0, System.currentTimeMillis());
					row.setField(1, StringData.fromString("b"));
//          row.setField(1, StringData.fromString(UUID.randomUUID().toString()));
					ctx.collect(row);
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

    	inputStream.print();

		env.execute("iceberg write");

	}

}

