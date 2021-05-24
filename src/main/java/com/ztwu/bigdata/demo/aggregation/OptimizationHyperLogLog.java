package com.ztwu.bigdata.demo.aggregation;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.util.HashSet;
import java.util.Set;

public class OptimizationHyperLogLog {
	//hyperloglog 结构
	private HyperLogLog hyperLogLog;
	//初始的一个 set
	private Set<Integer> set;

	private double rsd;

	//hyperloglog 的桶个数，主要内存占用
	private int bucket;

	public OptimizationHyperLogLog(double rsd){
		this.rsd=rsd;
//		this.bucket=1 << HyperLogLog.log2m(rsd);
		set=new HashSet<>();
	}

	//插入一条数据
	public void offer(Object object){
		final int x = MurmurHash.hash(object);
		int currSize=set.size();
		if(hyperLogLog==null && currSize+1>bucket){
			//升级为 hyperloglog
			hyperLogLog=new HyperLogLog(rsd);
			for(int d: set){
				hyperLogLog.offerHashed(d);
			}
			set.clear();
		}

		if(hyperLogLog!=null){
			hyperLogLog.offerHashed(x);
		}else {
			set.add(x);
		}
	}

	//获取大小
	public long cardinality() {
		if(hyperLogLog!=null) return hyperLogLog.cardinality();
		return set.size();
	}
}