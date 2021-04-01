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
				+ "create table source_kafka "
				+ "( "
				+ "    userID STRING, "
				+ "    eventType STRING, "
				+ "    eventTime STRING, "
				+ "    proctime AS PROCTIME() "
				+ ") with ( "
				+ "    'connector.type' = 'kafka', "
				+ "    'connector.version' = '0.10', "
				+ "    'connector.properties.bootstrap.servers' = 'kafka01:9092', "
				+ "    'connector.properties.zookeeper.connect' = 'kafka01:2181', "
				+ "    'connector.topic' = 'test_1', "
				+ "    'connector.properties.group.id' = 'c1_test_1', "
				+ "    'connector.startup-mode' = 'latest-offset', "
				+ "    'format.type' = 'json' "
				+ ")";
		tableEnv.sqlUpdate(sourceDDL);
		//tableEnv.toAppendStream(tableEnv.from("source_kafka"), Row.class).print();

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
				+ "    userID STRING, "
				+ "    userName STRING, "
				+ "    userAge INT "
				+ ") WITH ( "
				+ "    'connector.type' = 'jdbc', "
				+ "    'connector.url' = 'jdbc:mysql://localhost:3306/bigdata', "
				+ "    'connector.table' = 't_user_info', "
				+ "    'connector.driver' = 'com.mysql.jdbc.Driver', "
				+ "    'connector.username' = '****', "
				+ "    'connector.password' = '******' "
				+ ")";
		tableEnv.sqlUpdate(dimDDL);

		// Query
		// Left Join
		String execSQL = ""
				+ "SELECT "
				+ "  kafka.*,mysql.userName,mysql.userAge "
				+ "FROM "
				+ "  source_kafka as kafka"
				+ "  LEFT JOIN dim_mysql FOR SYSTEM_TIME AS OF kafka.proctime AS mysql "
				+ "  ON kafka.userID = mysql.userID";
		Table table = tableEnv.sqlQuery(execSQL);
		tableEnv.toAppendStream(table, Row.class).print();

		tableEnv.execute(LookupableTableSource.class.getSimpleName());

	}
}