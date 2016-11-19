package com.alibaba.middleware.race.index.model;


public class OrderposTime implements Comparable<OrderposTime>{
	
	private OrderPosInfo posInfo;
	
	private Long createTime;
	
	public OrderposTime(){
		
	}
	
	public OrderposTime(OrderPosInfo posInfo,long createTime)
	{
		this.posInfo = posInfo;
		this.createTime = createTime;
	}


	public OrderPosInfo getPosInfo() {
		return posInfo;
	}

	public void setPosInfo(OrderPosInfo posInfo) {
		this.posInfo = posInfo;
	}

	public Long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	
	@Override
	public int compareTo(OrderposTime o) {
		return this.createTime.compareTo(o.createTime);
	}
	
	

}
