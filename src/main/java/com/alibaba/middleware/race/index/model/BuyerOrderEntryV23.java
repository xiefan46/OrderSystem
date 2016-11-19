package com.alibaba.middleware.race.index.model;


public class BuyerOrderEntryV23 implements Comparable<BuyerOrderEntryV23>{
	
	private String buyerid;
	
	private long pos;
	
	private long createtime;
	
	
	public BuyerOrderEntryV23(String buyerid, long pos,long createtime) {
		super();
		this.buyerid = buyerid;
		this.pos = pos;
		this.createtime = createtime;
	}

	public String getBuyerid() {
		return buyerid;
	}


	public void setBuyerid(String buyerid) {
		this.buyerid = buyerid;
	}

	public long getPos() {
		return pos;
	}


	public void setPos(long pos) {
		this.pos = pos;
	}



	public long getCreatetime() {
		return createtime;
	}

	public void setCreatetime(long createtime) {
		this.createtime = createtime;
	}

	@Override
	public int compareTo(BuyerOrderEntryV23 o) {
		return this.buyerid.compareTo(o.buyerid);
	}
}
