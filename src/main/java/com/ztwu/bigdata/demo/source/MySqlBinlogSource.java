package com.ztwu.bigdata.demo.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

public class MySqlBinlogSource {
  public static void main(String[] args) throws Exception {
    SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
      .hostname("192.168.56.101")
      .port(3306)
      .databaseList("test") // monitor all tables under inventory database
      .username("root")
      .password("root")
      .tableList("test.user")
      .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
      .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env
      .addSource(sourceFunction)
      .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
    env.execute();
  }
}