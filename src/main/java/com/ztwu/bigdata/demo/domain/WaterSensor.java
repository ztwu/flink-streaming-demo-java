package com.ztwu.bigdata.demo.domain;

public class WaterSensor {
	public String id;
	public long ts;
	public int vc;

	public WaterSensor() {
	}

	public WaterSensor(String id, Long ts, int vc) {
		this.id = id;
		this.ts = ts;
		this.vc = vc;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public int getVc() {
		return vc;
	}

	public void setVc(int vc) {
		this.vc = vc;
	}
}
