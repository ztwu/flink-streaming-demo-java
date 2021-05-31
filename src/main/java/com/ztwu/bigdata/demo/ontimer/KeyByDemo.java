package com.ztwu.bigdata.demo.ontimer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 只有keyedStream在使用ProcessFunction时可以使用State和Timer定时器
 */
public class KeyByDemo {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//		env.setParallelism(10);
		//1000,hello
//		DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
		DataStream<String> lines = env.addSource(new RichSourceFunction<String>() {

			private static final long serialVersionUID = 1L;
			boolean flag = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (flag) {
					int n = new Random().nextInt(4);
					if(n==1){
						ctx.collect(System.currentTimeMillis()+","+"ABCDEa123abc");
					}else if(n==2){
						ctx.collect(System.currentTimeMillis()+","+"ABCDFB123abc");
					}else if(n==3){
						ctx.collect(System.currentTimeMillis()+","+"999999999999");
					}else if(n==0){
						ctx.collect(System.currentTimeMillis()+","+"ztwuwwwwwwww");
					}
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String line) throws Exception {
				String word = line.split(",")[1];
				return Tuple2.of(word, 1);
			}
		});

		//调用keyBy进行分组
		KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne
				.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
					@Override
					public String getKey(Tuple2<String, Integer> value) throws Exception {
//						return value.f0;
						return new StringBuffer(value.f0).reverse().toString().substring(0,2);
					}
				});

		//没有划分窗口，直接调用底层的process方法
		keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			private transient MapState<String, Integer> bufferState;
			// 定义状态描述器
			@Override
			public void open(Configuration parameters) throws Exception {
				MapStateDescriptor<String, Integer> listStateDescriptor = new MapStateDescriptor<>(
						"mapstate", String.class, Integer.class);
				bufferState = getRuntimeContext().getMapState(listStateDescriptor);
			}

			@Override
			public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
				String nkey = ctx.getCurrentKey();
				String key = value.f0;
				System.out.println(nkey+" : "+key);
				bufferState.put(key,1);

				System.out.println(bufferState.keys());
				out.collect(value);
			}
		}).print();

		env.execute();


	}
}
