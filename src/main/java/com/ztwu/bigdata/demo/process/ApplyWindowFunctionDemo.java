package com.ztwu.bigdata.demo.process;

import com.alibaba.fastjson.JSON;
import com.ztwu.bigdata.demo.domain.ItemViewCount;
import com.ztwu.bigdata.demo.domain.MyBehavior;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * apply是在窗口内进行全量的聚合，浪费资源
 */
public class ApplyWindowFunctionDemo {

	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(60000);
		env.setParallelism(1);
		//json字符串
		DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

		SingleOutputStreamOperator<MyBehavior> behaviorDataStream = lines.process(new ProcessFunction<String, MyBehavior>() {
			@Override
			public void processElement(String value, Context ctx, Collector<MyBehavior> out) throws Exception {
				try {
					MyBehavior behavior = JSON.parseObject(value, MyBehavior.class);
					//输出
					out.collect(behavior);
				} catch (Exception e) {
					//e.printStackTrace();
					//TODO 记录出现异常的数据
				}
			}
		});

		//提取EventTime生成WaterMark
		SingleOutputStreamOperator<MyBehavior> behaviorDataStreamWithWaterMark = behaviorDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyBehavior>(Time.seconds(0)) {
			@Override
			public long extractTimestamp(MyBehavior element) {
				return element.getTimestamp();
			}
		});

		//按照指定的字段进行分组
		KeyedStream<MyBehavior, Tuple> keyed = behaviorDataStreamWithWaterMark.keyBy("itemId", "type");

		//窗口长度为10分组，一分钟滑动一次
		WindowedStream<MyBehavior, Tuple, TimeWindow> window = keyed.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));

		//SingleOutputStreamOperator<MyBehavior> sum = window.sum("counts");
		SingleOutputStreamOperator<ItemViewCount> sum = window.apply(new WindowFunction<MyBehavior, ItemViewCount, Tuple, TimeWindow>() {

			//当窗口触发是，会调用一次apply方法，相当于是对窗口中的全量数据进行计算
			@Override
			public void apply(Tuple tuple, TimeWindow window, Iterable<MyBehavior> input, Collector<ItemViewCount> out) throws Exception {
				//窗口的起始时间
				long start = window.getStart();
				//窗口的结束时间
				long end = window.getEnd();
				//获取分组的key
				String itemId = tuple.getField(0);
				String type = tuple.getField(1);

				int count = 0;
				for (MyBehavior myBehavior : input) {
					count++;
				}
				//输出结果
				out.collect(new ItemViewCount(itemId, type, start, end, count++));
			}
		});

		sum.print();

		env.execute();

	}
}
