package com.ztwu.bigdata.demo.dim;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
public class JoinDemoAsyncIoRedis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        DataStream<Tuple3<String,Integer, String>> orderedResult = AsyncDataStream
                //保证顺序：异步返回的结果保证顺序，超时时间1秒，最大容量2，超出容量触发反压
                .orderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        DataStream<Tuple3<String,Integer, String>> unorderedResult = AsyncDataStream
                //允许乱序：异步返回的结果允许乱序，超时时间1秒，最大容量2，超出容量触发反压
                .unorderedWait(textStream, new JoinDemo3AyncFunction(), 1000L, TimeUnit.MILLISECONDS, 2)
                .setParallelism(1);

        orderedResult.print();
        unorderedResult.print();
        env.execute("joinDemo");
    }

    //定义个类，继承RichAsyncFunction，实现异步查询存储在redis里的维表
    //输入用户名、城市ID，返回 Tuple3<用户名、城市ID，城市名称>
    static class JoinDemo3AyncFunction extends RichAsyncFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {

        private transient RedisClient redisClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            RedisOptions config = new RedisOptions();
            config.setHost("127.0.0.1");
            config.setPort(6379);

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);

            Vertx vertx = Vertx.vertx(vo);

            redisClient = RedisClient.create(vertx, config);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if(redisClient!=null)
                redisClient.close(null);

        }

        //异步查询方法
        @Override
        public void asyncInvoke(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            // 使用 city id 查询
            redisClient.get(input.f1.toString(), getRes->{
                if(getRes.succeeded()){
                    String result = getRes.result();
                    if(result== null){
                        resultFuture.complete(null);
                        return;
                    }
                    else {
                        List list = new ArrayList<Tuple2<Integer, String>>();
                        list.add(new Tuple3<>(input.f0, input.f1, result));
                        resultFuture.complete(list);
                    }
                } else if(getRes.failed()){
                    resultFuture.complete(null);
                    return;
                }

            });

        }

        //超时处理
        @Override
        public void timeout(Tuple2<String, Integer> input, ResultFuture<Tuple3<String,Integer, String>> resultFuture) throws Exception {
            List list = new ArrayList<Tuple2<Integer, String>>();
            list.add(new Tuple3<>(input.f0,input.f1, ""));
            resultFuture.complete(list);
        }
    }
}
