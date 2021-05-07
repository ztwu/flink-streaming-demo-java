package com.ztwu.bigdata.demo.domain;

public class ItemViewCount {
	String itemId;
	String type;
	long windowEndStart;
	long windowEnd;
	long viewCount;

	public ItemViewCount(String itemId, String type, long windowEndStart, long windowEnd, long viewCount) {
		this.itemId = itemId;
		this.type = type;
		this.windowEndStart = windowEndStart;
		this.windowEnd = windowEnd;
		this.viewCount = viewCount;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getWindowEndStart() {
		return windowEndStart;
	}

	public void setWindowEndStart(long windowEndStart) {
		this.windowEndStart = windowEndStart;
	}

	public long getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(long windowEnd) {
		this.windowEnd = windowEnd;
	}

	public long getViewCount() {
		return viewCount;
	}

	public void setViewCount(long viewCount) {
		this.viewCount = viewCount;
	}
}
