
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency

CREATE TABLE MySQLTable (
  ...
) WITH (
  'connector.type' = 'jdbc', -- 必选: jdbc方式
  'connector.url' = 'jdbc:mysql://localhost:3306/flink-test', -- 必选: JDBC url
  'connector.table' = 'jdbc_table_name',  -- 必选: 表名
   -- 可选: JDBC driver，如果不配置，会自动通过url提取
  'connector.driver' = 'com.mysql.jdbc.Driver',
  'connector.username' = 'name', -- 可选: 数据库用户名
  'connector.password' = 'password',-- 可选: 数据库密码
    -- 可选, 将输入进行分区的字段名.
  'connector.read.partition.column' = 'column_name',
    -- 可选, 分区数量.
  'connector.read.partition.num' = '50',
    -- 可选, 第一个分区的最小值.
  'connector.read.partition.lower-bound' = '500',
    -- 可选, 最后一个分区的最大值
  'connector.read.partition.upper-bound' = '1000',
    -- 可选, 一次提取数据的行数，默认为0，表示忽略此配置
  'connector.read.fetch-size' = '100',
   -- 可选, lookup缓存数据的最大行数，如果超过改配置，老的数据会被清除
  'connector.lookup.cache.max-rows' = '5000',
   -- 可选，lookup缓存存活的最大时间，超过该时间旧数据会过时，注意cache.max-rows与cache.ttl必须同时配置
  'connector.lookup.cache.ttl' = '10s',
   -- 可选, 查询数据最大重试次数
  'connector.lookup.max-retries' = '3',
   -- 可选,写数据最大的flush行数，默认5000，超过改配置，会触发刷数据
  'connector.write.flush.max-rows' = '5000',
   --可选，flush数据的间隔时间，超过该时间，会通过一个异步线程flush数据，默认是0s
  'connector.write.flush.interval' = '2s',
  -- 可选, 写数据失败最大重试次数
  'connector.write.max-retries' = '3'
)