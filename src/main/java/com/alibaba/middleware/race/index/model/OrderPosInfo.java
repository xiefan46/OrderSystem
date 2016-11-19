package com.alibaba.middleware.race.index.model;

public class OrderPosInfo {
	
	private long orderpos;
	
	private int diskId;
	
	public OrderPosInfo(long orderpos,int diskId)
	{
		this.orderpos = orderpos;
		this.diskId = diskId;
	}

	

	public long getOrderpos() {
		return orderpos;
	}



	public void setOrderpos(long orderpos) {
		this.orderpos = orderpos;
	}



	public int getDiskId() {
		return diskId;
	}

	public void setDiskId(int diskId) {
		this.diskId = diskId;
	}
	
	
}
