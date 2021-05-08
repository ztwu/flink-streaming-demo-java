package com.ztwu.bigdata.demo.process;

import com.ztwu.bigdata.demo.domain.CountWithTimestamp;
import com.ztwu.bigdata.demo.domain.UserAction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutProcessingTimeFunction
		extends KeyedProcessFunction<Integer, UserAction, Tuple2<Integer, Long>> {

	private ValueState<CountWithTimestamp> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
	}

	@Override
	public void processElement(
			UserAction value,
			Context ctx,
			Collector<Tuple2<Integer, Long>> out) throws Exception {

		CountWithTimestamp current = state.value();

		if (current == null) {
			current = new CountWithTimestamp();
			current.key = value.getUserId();
		}

		current.count++;
		current.lastModified = ctx.timestamp();

		state.update(current);
		// 注册ProcessingTime处理时间的定时器
		ctx.timerService().registerProcessingTimeTimer(current.lastModified + 60000L);
	}

	@Override
	public void onTimer(
			long timestamp,
			OnTimerContext ctx,
			Collector<Tuple2<Integer, Long>> out) throws Exception {

		CountWithTimestamp result = state.value();

		if (timestamp == result.lastModified + 60000L) {
			out.collect(new Tuple2<Integer, Long>(result.key, result.count));
			state.update(null);
			ctx.timerService().deleteEventTimeTimer(timestamp);
		}
	}
}