package com.ztwu.bigdata.demo.ontimer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * 只有keyedStream在使用ProcessFunction时可以使用State和Timer定时器
 */
public class ProcessFunctionWithOnTimerByEventTime {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
				}

			}

			@Override
			public void cancel() {
				flag = false;
			}
		});

		SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
			@Override
			public long extractTimestamp(String element) {
				return Long.parseLong(element.split(",")[0]);
			}
		});

		SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
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

			private transient ListState<Tuple2<String, Integer>> bufferState;

			@Override
			public void open(Configuration parameters) throws Exception {
				ListStateDescriptor<Tuple2<String, Integer>> listStateDescriptor = new ListStateDescriptor<Tuple2<String, Integer>>(
						"list-state",
						TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){})
				);

				bufferState = getRuntimeContext().getListState(listStateDescriptor);
			}

			@Override
			public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

				//out.collect(value);

				bufferState.add(value);
				//获取当前的event time
				Long timestamp = ctx.timestamp();

				//10:14:13   ->   10:15:00
				//输入的时间 [10:14:00, 10:14:59) 注册的定时器都是 10:15:00
				System.out.println("current event time is : " + timestamp);

				//注册定时器，如果注册的是EventTime类型的定时器，当WaterMark大于等于注册定时器的时间，就会触发onTimer方法
				long timer = timestamp - timestamp % 60000 + 60000;
				System.out.println("next timer is: " + timer);
				ctx.timerService().registerEventTimeTimer(timer);

//				//注册定时器，如果注册的是ProcessingTime类型的定时器，当SubTask所在机器的ProcessingTime大于等于注册定时器的时间，就会触发onTimer方法
//				long timer = currentProcessingTime - currentProcessingTime % 60000 + 60000;
//				System.out.println("next timer is: " + timer);
//				//注册ProcessingTime的定时器
//				ctx.timerService().registerProcessingTimeTimer(timer);

			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

				System.out.println("触发定时器："+timestamp);
				Iterable<Tuple2<String, Integer>> iterable = bufferState.get();

				for (Tuple2<String, Integer> tp : iterable) {
					out.collect(tp);
				}

				//请求当前ListState中的数据
				bufferState.clear();
			}
		}).print();

		env.execute();


	}
}
