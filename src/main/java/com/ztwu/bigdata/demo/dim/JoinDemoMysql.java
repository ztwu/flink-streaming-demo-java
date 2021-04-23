package com.ztwu.bigdata.demo.dim;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * (2) 使用异步IO来提高访问吞吐量
 *
 * Flink中可以使用异步IO来读写外部系统，这要求外部系统客户端支持异步IO，
 * 不过目前很多系统都支持异步IO客户端。但是如果使用异步就要涉及到三个问题：
 *
 * 超时：如果查询超时那么就认为是读写失败，需要按失败处理；
 * 并发数量：如果并发数量太多，就要触发Flink的反压机制来抑制上游的写入。
 * 返回顺序错乱：顺序错乱了要根据实际情况来处理，Flink支持两种方式：允许乱序、保证顺序。
 *
 **/
public class JoinDemoMysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 添加实时流
        DataStreamSource<Tuple2<String, String>> stream = env.fromElements(
                Tuple2.of("01", "click"),
                Tuple2.of("02", "click"),
                Tuple2.of("03", "browse"));

        // 关联维度
        SingleOutputStreamOperator<String> dimedStream = stream.flatMap(new JoinDemo3Function());
        dimedStream.print();

        env.execute();
    }

    static class JoinDemo3Function extends RichFlatMapFunction<Tuple2<String, String>, String> {

        private static String jdbcUrl = "jdbc:mysql://192.168.56.101:3306?useSSL=false";
        private static String username = "root";
        private static String password = "root";
        private static String driverName = "com.mysql.jdbc.Driver";

        AtomicReference<Map<String,String>> dim = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            dim = new AtomicReference(new HashMap<String, String>());
            dim.set(query());
            ScheduledExecutorService executors= Executors.newSingleThreadScheduledExecutor();
            executors.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    dim.set(query());
                }
            }, 10000, 86400, TimeUnit.SECONDS);
        }

        // 关联维度
        @Override
        public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
            if (dim.get().containsKey(value.f0)) {
                String name = dim.get().get(value.f0);
                out.collect(value.f0 + "," + value.f1 + "," + name);
            }
        }

        private Map<String,String> query() {
            Connection conn = null;
            Map<String,String> initMap = new ConcurrentHashMap<>();
            try {
                Class.forName(driverName);
                conn = DriverManager.getConnection(jdbcUrl,username,password);
                PreparedStatement ps = conn.prepareStatement("select id,name from test.user");
                ResultSet rs = ps.executeQuery();
                while (!rs.isClosed() && rs.next()) {
                    String id = rs.getString(1);
                    String name = rs.getString(2);
                    // 将结果放到 map 中
                    initMap.put(id, name);
                }
                System.out.println(initMap);
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            return initMap;
        }
    }
}
