package com.ztwu.bigdata.demo.domain;

import java.text.SimpleDateFormat;

public class People {

	String age;
	long eventTime;
	String eventTimeStr;
	String id;
	String name;


	public People(String age, long eventTime, String id, String name) {
		this.age = age;
		this.eventTime = eventTime;
		this.id = id;
		this.name = name;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public String getEventTimeStr() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return simpleDateFormat.format(eventTime);
	}

	public void setEventTimeStr(String eventTimeStr) {
		this.eventTimeStr = eventTimeStr;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}