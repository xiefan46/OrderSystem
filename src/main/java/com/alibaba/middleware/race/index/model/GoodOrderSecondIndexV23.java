package com.alibaba.middleware.race.index.model;

import java.util.ArrayList;
import java.util.TreeSet;

public class GoodOrderSecondIndexV23 {
	
	private String goodid = null;
	
	private ArrayList<OrderPosInfo> items;
	
	public GoodOrderSecondIndexV23(){
		this.items = new ArrayList<OrderPosInfo>();
	}
	
	public void addEntry(GoodOrderEntryV23 entry,int diskId) throws Exception
	{
		if(this.goodid == null){
			this.goodid = entry.getGoodid();
		}
		if(!this.goodid.equals(entry.getGoodid())){
			throw new Exception("buyerid不匹配");
		}
		this.items.add(new OrderPosInfo(entry.getPos(), diskId));
	}
	

	public String getGoodid() {
		return goodid;
	}

	public void setGoodid(String goodid) {
		this.goodid = goodid;
	}

	
	
	public ArrayList<OrderPosInfo> getItems() {
		return items;
	}

	public void setItems(ArrayList<OrderPosInfo> items) {
		this.items = items;
	}

	public boolean isEmpty()
	{
		return this.items.isEmpty();
	}
	
	
}
