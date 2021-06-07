package com.ztwu.bigdata.demo.aggregation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

/**
 *  ValueState<T> ：这个状态为每一个 key 保存一个值
 *      value() 获取状态值
 *      update() 更新状态值
 *      clear() 清除状态
 *
 *      IN,输入的数据类型
 *      OUT：数据出的数据类型
 */
public class CountAverageWithReduceState
		extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

	private ReducingState<Long> reducingState;

	/***状态初始化*/
	@Override
	public void open(Configuration parameters) throws Exception {

		ReducingStateDescriptor descriptor = new ReducingStateDescriptor("ReducingDescriptor", new ReduceFunction<Long>() {
			@Override
			public Long reduce(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		},Long.class);
		reducingState = getRuntimeContext().getReducingState(descriptor);
	}

	@Override
	public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> collector) throws Exception {

		//将状态放入
		reducingState.add(element.f1);
		collector.collect(Tuple2.of(element.f0,reducingState.get()));

	}


}