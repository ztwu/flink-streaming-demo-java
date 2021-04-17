-- 一、常规join
-- 需要将所有的历史记录存放到state中，所有历史数据都参与join。
-- 对于新增的记录或者是原有记录的更新操作，join两边的表都是可见的，
-- 也就是说要和所有的记录进行关联计算。比如左表A新增一条记录，
-- 那么这条记录要和右表所有记录进行关联（历史数据、未来的数据）。
-- 缺点：state会无限增大
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id

-- 二、时间窗口join
-- 由于常规join的状态会无限增大，那么限制只join一段时间的数据，
-- 这就是时间窗口join。Flink会从state中删除过期的数据
SELECT *
FROM
  Orders o,
  Shipments s
WHERE
  o.id = s.orderId AND
  s.shiptime BETWEEN o.ordertime AND o.ordertime + INTERVAL '4' HOUR

-- 三、时态表join
-- 时态表可以是一张跟踪所有变更记录的表（例如数据库表的 changelog，包含多个表快照），
-- 也可以是物化所有变更之后的表（例如数据库表，只有最新表快照）。
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1

-- 基于事件时间的时态 Join
-- 使用场景：需要查询被关联表（一般是维表）的历史版本。
-- 例如订单表去关联价格表，需要去下单时间对于版本的价格。
-- left join时使用主表的事件时间（如下单时间）。

SELECT * FROM product_changelog;

(changelog kind)  update_time product_name price
================= =========== ============ =====
+(INSERT)         00:01:00    scooter      11.11
+(INSERT)         00:02:00    basketball   23.11
-(UPDATE_BEFORE)  12:00:00    scooter      11.11
+(UPDATE_AFTER)   12:00:00    scooter      12.99  <= 产品 `scooter` 在 `12:00:00` 时涨价到了 `12.99`
-(UPDATE_BEFORE)  12:00:00    basketball   23.11
+(UPDATE_AFTER)   12:00:00    basketball   19.99  <= 产品 `basketball` 在 `12:00:00` 时降价到了 `19.99`
-(DELETE)         18:00:00    scooter      12.99  <= 产品 `scooter` 在 `18:00:00` 从数据库表中删除
-- 如果我们想输出 product_changelog 表在 10:00:00 对应的版本，表的内容如下所示：

update_time  product_id product_name price
===========  ========== ============ =====
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
-- 如果我们想输出 product_changelog 表在 13:00:00 对应的版本，表的内容如下所示：

update_time  product_id product_name price
===========  ========== ============ =====
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
-- 通过基于事件时间的时态表 join, 我们可以 join 上版本表中的不同版本：

CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time  -- defines the necessary event time
) WITH (
...
);

-- 设置会话的时间区间, changelog 里的数据库操作时间是以 epoch 开始的毫秒数存储的，
-- 在从毫秒转化为时间戳时，Flink SQL 会使用会话的时间区间
-- 因此，请根据 changelog 中的数据库操作时间设置合适的时间区间
SET table.local-time-zone=UTC;

-- 声明一张版本表
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL, -- 注意：自动从毫秒数转为时间戳
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) defines the primary key constraint
  WATERMARK FOR update_time AS update_time   -- (2) defines the event time by watermark
) WITH (
  'connector' = 'kafka',
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);

-- 基于事件时间的时态表 Join
SELECT
  order_id,
  order_time,
  product_name,
  product_time,
  price
FROM orders AS O
LEFT JOIN product_changelog FOR SYSTEM_TIME AS OF O.order_time AS P
ON O.product_id = P.product_id;

order_id order_time product_name product_time price
======== ========== ============ ============ =====
o_001    00:01:00   scooter      00:01:00     11.11
o_002    00:03:00   basketball   00:02:00     23.11
o_003    12:00:00   scooter      12:00:00     12.99
o_004    12:00:00   basketball   12:00:00     19.99
o_005    18:00:00   NULL         NULL         NULL

-- 基于处理时间的时态 Join
-- 使用场景：只需查询被关联表的最新状态，不查历史版本。
-- 例如计算当天总金额，需要将不同币种装换成统一币种，转换时汇率使用最新汇率。

SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
-- 基于以上，我们想要计算所有 Orders 表的订单金额总和，并同时转换为对应成日元的金额。

-- 例如，我们想要以表 LatestRates 中的汇率将以下订单转换，则结果将为：

amount currency     rate   amout*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
-- 通过时态表 Join，我们可以将上述操作表示为以下 SQL 查询：

SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency

-- 四、时态表函数join
-- 使用场景：通过时态表函数取出最新状态数据，进行关联计算。

SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency