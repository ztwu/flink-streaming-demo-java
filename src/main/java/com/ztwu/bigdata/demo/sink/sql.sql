CREATE TABLE student(
  id INT,
  name STRING,
  password STRING,
  age INT,
  ts BIGINT,
  eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')), -- 事件时间
  WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECOND -- 水印
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal', -- 指定Kafka连接器版本，不能为2.4.0，必须为universal，否则会报错
  'connector.topic' = 'student', -- 指定消费的topic
  'connector.startup-mode' = 'latest-offset', -- 指定起始offset位置
  'connector.properties.zookeeper.connect' = 'hadoop000:2181',
  'connector.properties.bootstrap.servers' = 'hadooop000:9092',
  'connector.properties.group.id' = 'student_1',
  'format.type' = 'json',
  'format.derive-schema' = 'true', -- 由表schema自动推导解析JSON
  'update-mode' = 'append'
);

CREATE TABLE pvuv_sink (
    dt VARCHAR,
    pv BIGINT,
    uv BIGINT
) WITH (
    'connector.type' = 'jdbc', -- 使用 jdbc connector
    'connector.url' = 'jdbc:mysql://localhost:3306/flink-test', -- jdbc url
    'connector.table' = 'pvuv_sink', -- 表名
    'connector.username' = 'root', -- 用户名
    'connector.password' = '123456', -- 密码
    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
)

create table hive_sink_table (
 user_name string,
 user_id bigint,
 `time` bigint,
 `date` string
)
partitioned by (`date` string)
row format delimited fields terminated by '\t'
stored as orc
location 'hdfs://BigdataCluster/user/hive/warehouse/test_data.db/test/test_hive_table'
tblproperties (
 'orc.compress'='SNAPPY',
 'partition.time-extractor.timestamp-pattern' = '$date 00:00:00',
 'sink.partition-commit.trigger' = 'process-time',
 'sink.partition-commit.policy.kind' = 'metastore,success-file'
)