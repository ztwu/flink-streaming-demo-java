package com.ztwu.bigdata.demo.dim;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *
 * 需要实现LookupableTableSource接口。
 * 比较通用。
 * 依赖外部存储，当数据变化时，可及时获取。
 * 目前仅支持Blink Planner。
 * 但是不支持基于eventTime关联历史状态维度表
 *
 *  Kafka Join Mysql-Dim
 */
public class LookupableTableSource {

	public static void main(String[] args) throws Exception {

		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

		// Source DDL
		// Kafka数据: {"userID":"user_1","eventType":"click","eventTime":"2015-01-01 00:00:00"}
		String sourceDDL = ""
				+ "create table source_kafka3 "
				+ "( "
				+ "    userID STRING, "
				+ "    eventType STRING, "
				+ "    eventTime STRING, "
				+ "    proctime AS PROCTIME() "
				+ ") with ( "
				+ "    'connector.type' = 'kafka', "
				+ "    'connector.version' = 'universal', "
				+ "    'connector.properties.bootstrap.servers' = '192.168.56.101:9092,192.168.56.101:9093,192.168.56.101:9094', "
				+ "    'connector.properties.zookeeper.servers' = '192.168.56.101:2181', "
				+ "    'connector.topic' = 'kafkademo3', "
				+ "    'connector.properties.group.id' = 'c1_test_111', "
				+ "    'connector.startup-mode' = 'group-offsets', "
				+ "    'format.type' = 'csv', "
				+ "    'format.field-delimiter' = ',', "
				+ "    'format.ignore-parse-errors' = 'false' "
				+ ")";
		tableEnv.executeSql(sourceDDL);
//		tableEnv.executeSql("select * from source_kafka3").print();

		// Dim DDL
		// Mysql维度数据
		// mysql> select * from t_user_info limit 1;
		// +--------+----------+---------+
		// | userID | userName | userAge |
		// +--------+----------+---------+
		// | user_1 | name1    |      10 |
		// +--------+----------+---------+
		String dimDDL = ""
				+ "CREATE TABLE dim_mysql ( "
				+ "    name STRING, "
				+ "    age STRING, "
				+ "    sex STRING, "
				+ "    id STRING "
				+ ") WITH ( "
				+ "    'connector.type' = 'jdbc', "
				+ "    'connector.url' = 'jdbc:mysql://192.168.56.101:3306/test', "
				+ "    'connector.table' = 'user', "
				+ "    'connector.driver' = 'com.mysql.jdbc.Driver', "
				+ "    'connector.username' = 'root', "
				+ "    'connector.password' = 'root' "
				+ ")";
		tableEnv.executeSql(dimDDL);
//		tableEnv.executeSql("select * from dim_mysql").print();

		String sinkDDL = ""
				+ "create table sink_kafka3 "
				+ "( "
				+ "    userID STRING, "
				+ "    eventType STRING, "
				+ "    eventTime STRING, "
				+ "    name STRING, "
				+ "    age STRING "
				+ ") with ( "
				+ "    'connector.type' = 'kafka', "
				+ "    'connector.version' = 'universal', "
				+ "    'connector.properties.bootstrap.servers' = '192.168.56.101:9092,192.168.56.101:9093,192.168.56.101:9094', "
				+ "    'connector.properties.zookeeper.servers' = '192.168.56.101:2181', "
				+ "    'connector.topic' = 'kafkademo3sink', "
				+ "    'format.type' = 'csv', "
				+ "    'format.field-delimiter' = ',', "
				+ "    'update-mode' = 'append' "
				+ ")";
		tableEnv.executeSql(sinkDDL);
//
//		// Query
//		// Left Join
		String execSQL = "insert into sink_kafka3 "
				+ "SELECT "
				+ "  kafka.userID,kafka.eventType,kafka.eventTime,mysql.name,mysql.age "
				+ "FROM "
				+ "  source_kafka3 as kafka"
				+ "  LEFT JOIN dim_mysql FOR SYSTEM_TIME AS OF kafka.proctime AS mysql "
				+ "  ON kafka.userID = mysql.id";
		tableEnv.executeSql(execSQL).print();

	}
}