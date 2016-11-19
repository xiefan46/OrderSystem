package com.alibaba.middleware.race.index.model;

public class GoodOrderEntryV23 implements Comparable<GoodOrderEntryV23>{
	private String goodid;
	
	private long pos;


	public GoodOrderEntryV23(String goodid, long pos) {
		this.goodid = goodid;
		this.pos = pos;
	}

	public String getGoodid() {
		return goodid;
	}

	public void setGoodid(String goodid) {
		this.goodid = goodid;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	@Override
	public int compareTo(GoodOrderEntryV23 o) {
		return goodid.compareTo(o.goodid);
	}
	
	

	
}
