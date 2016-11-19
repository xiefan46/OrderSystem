package com.alibaba.middleware.race.index.model;


public class StoreInfo {
	
	private long offset; //offset一定要用long
	
	private int length; //单条记录长度不超过2G
	
	public StoreInfo(long offset,int length)
	{
		this.offset = offset;
		this.length = length;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}
	
}
