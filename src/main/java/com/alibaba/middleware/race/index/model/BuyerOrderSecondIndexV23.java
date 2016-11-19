package com.alibaba.middleware.race.index.model;

import java.util.ArrayList;
import java.util.TreeSet;

public class BuyerOrderSecondIndexV23 {
	
	private String buyerid = null;
	
	private ArrayList<OrderposTime> items;
	
	public BuyerOrderSecondIndexV23(){
		this.items = new ArrayList<OrderposTime>();
	}
	
	public void addEntry(BuyerOrderEntryV23 entry,int diskId) throws Exception
	{
		if(this.buyerid == null){
			this.buyerid = entry.getBuyerid();
		}
		if(!this.buyerid.equals(entry.getBuyerid())){
			throw new Exception("buyerid不匹配");
		}
		this.items.add(new OrderposTime(new OrderPosInfo(entry.getPos(), diskId),entry.getCreatetime()));
	}
	

	public String getBuyerid() {
		return buyerid;
	}

	public void setBuyerid(String buyerid) {
		this.buyerid = buyerid;
	}
	
	public TreeSet<OrderposTime> getTreeSet()
	{
		TreeSet<OrderposTime> set = new TreeSet<OrderposTime>();
		for(OrderposTime oit : items)
			set.add(oit);
		return set;
	}
	
	public boolean isEmpty()
	{
		return this.items.isEmpty();
	}

	public ArrayList<OrderposTime> getItems() {
		return items;
	}

	public void setItems(ArrayList<OrderposTime> items) {
		this.items = items;
	}
	
	
}
