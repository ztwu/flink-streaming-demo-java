package com.ztwu.bigdata.demo.dim;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

/**
 *
 * 优点：维度数据量可以很大，维度数据更新及时，不依赖外部存储，可以关联不同版本的维度数据。
 * 缺点：只支持在Flink SQL API中使用。
 *
 * 在SQL中使用LATERAL TABLE语法和UDTF运行的结果进行关联。
 *
 * (1) ProcessingTime的一个实例
 **/
public class JoinDemoTemporalTableProcessingTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        //定义主流
        DataStream<Tuple2<String, Integer>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：user,1000,分别是用户名称和城市编号
                    String[] list = p.split(",");
                    return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        //定义城市流
        DataStream<Tuple2<Integer, String>> cityStream = env.socketTextStream("localhost", 9001, "\n")
                .map(p -> {
                    //输入格式为：城市ID,城市名称
                    String[] list = p.split(",");
                    return new Tuple2<Integer, String>(Integer.valueOf(list[0]), list[1]);
                })
                .returns(new TypeHint<Tuple2<Integer, String>>() {
                });

        //转变为Table
        Table userTable = tableEnv.fromDataStream(textStream, "user_name,city_id,ps.proctime");
        Table cityTable = tableEnv.fromDataStream(cityStream, "city_id,city_name,ps.proctime");

        //定义一个TemporalTableFunction
        TemporalTableFunction dimCity = cityTable.createTemporalTableFunction("ps", "city_id");
        //注册表函数
        tableEnv.registerFunction("dimCity", dimCity);

        //关联查询
        Table result = tableEnv
                .sqlQuery("select u.user_name,u.city_id,d.city_name from " + userTable + " as u " +
                        ", Lateral table (dimCity(u.ps)) d " +
                        "where u.city_id=d.city_id");

        //打印输出
        DataStream resultDs = tableEnv.toAppendStream(result, Row.class);
        resultDs.print();
        env.execute("joinDemo");
    }
}