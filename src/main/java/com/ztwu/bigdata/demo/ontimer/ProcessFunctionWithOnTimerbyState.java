package com.ztwu.bigdata.demo.ontimer;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

/**
 * 只有keyedStream在使用ProcessFunction时可以使用State和Timer定时器
 */
public class ProcessFunctionWithOnTimerbyState {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
					ctx.collect(System.currentTimeMillis()+","+"dog");
					Thread.sleep(1000);
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
		KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

		//没有划分窗口，直接调用底层的process方法
		keyed.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

			private transient MapState<String, Integer> bufferState;

			@Override
			public void open(Configuration parameters) throws Exception {
				MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>(
						"map-state",
						TypeInformation.of(String.class),
						TypeInformation.of(Integer.class)
				);

				bufferState = getRuntimeContext().getMapState(mapStateDescriptor);
				System.out.println(bufferState);

			}

			@Override
			public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

				long currentProcessingTime = System.currentTimeMillis();
				long startTime = currentProcessingTime - currentProcessingTime%(60*1000);
				long endTime = startTime + 60*1000;

				//out.collect(value);
				Integer num = bufferState.get(value.f0);
				if(num == null){
					bufferState.put(startTime+"_"+endTime+"="+value.f0,value.f1);
				}else{
					bufferState.put(startTime+"_"+endTime+"="+value.f0,num+value.f1);
				}

				//注册定时器，如果注册的是ProcessingTime类型的定时器，当SubTask所在机器的ProcessingTime大于等于注册定时器的时间，就会触发onTimer方法
				System.out.println("next timer is: " + endTime);
				//注册ProcessingTime的定时器
				ctx.timerService().registerProcessingTimeTimer(endTime);

			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

				System.out.println("触发定时器："+timestamp);
				bufferState.entries().forEach(value->{
					out.collect(Tuple2.of(value.getKey(),value.getValue()));
				});

				//请求当前ListState中的数据
				//计算指定时间内的统计值
				bufferState.clear();
			}
		}).print();

		env.execute();


	}
}
