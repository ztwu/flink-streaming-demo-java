package com.ztwu.bigdata.demo.domain;

import java.io.Serializable;

public class UserAction implements Serializable {
	private int userId;
	private String page;
	private String action;
	private long userActionTime;

	// 省略get set方法

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getPage() {
		return page;
	}

	public void setPage(String page) {
		this.page = page;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public long getUserActionTime() {
		return userActionTime;
	}

	public void setUserActionTime(long userActionTime) {
		this.userActionTime = userActionTime;
	}
}