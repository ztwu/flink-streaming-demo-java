package com.ztwu.bigdata.demo.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Version 1.0
 * @Desc
 */
public class JoinByProcessTime {

	private static final Logger LOG = LoggerFactory.getLogger(JoinByProcessTime.class);
	private static final String[] TYPE = {"a", "b", "c", "d"};

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//添加自定义数据源,每秒发出一笔订单信息{商品名称,商品数量}
		DataStreamSource<Tuple2<String, Integer>> orderSource1 = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			private volatile boolean isRunning = true;
			private final Random random = new Random();

			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				while (isRunning) {
					TimeUnit.SECONDS.sleep(1);
					Tuple2<String, Integer> tuple2 = Tuple2.of(TYPE[random.nextInt(TYPE.length)], random.nextInt(10));
					System.out.println(new Date() + ",orderSource1提交元素:" + tuple2);
					ctx.collect(tuple2);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}

		}, "orderSource1");

		DataStreamSource<Tuple2<String, Integer>> orderSource2 = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			private volatile boolean isRunning = true;
			private final Random random = new Random();

			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				while (isRunning) {
					TimeUnit.SECONDS.sleep(1);
					Tuple2<String, Integer> tuple2 = Tuple2.of(TYPE[random.nextInt(TYPE.length)], random.nextInt(10));
					System.out.println(new Date() + ",orderSource2提交元素:" + tuple2);
					ctx.collect(tuple2);
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}

		}, "orderSource2");

		orderSource1.join(orderSource2).where(new KeySelector<Tuple2<String, Integer>, String>() {
			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		}).equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		}).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
				return Tuple2.of(first.f0, first.f1 + second.f1);//计算key相同的属性1值的和
			}
		}).print();
		env.execute("Flink JoinByProcessTime");
	}
}