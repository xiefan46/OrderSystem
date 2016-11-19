package com.alibaba.middleware.race.index.model;

import java.util.ArrayList;
import java.util.Collections;

public class BuyerOrderFirstIndexV23 {
	
	private static final int BLOCK_SIZE = 16;
	
	private ArrayList<BuyerOrderSecondIndexV23> blocks = new ArrayList<BuyerOrderSecondIndexV23>();
	
	private String id = null;
	
	public BuyerOrderFirstIndexV23(){
		
	}
	
	public void putBlock(BuyerOrderSecondIndexV23 block)
	{
		this.blocks.add(block);
		if(this.id == null) this.id = block.getBuyerid();
	}
	
	public boolean isFull()
	{
		if(this.blocks.size() >= BLOCK_SIZE) return true;
		return false;
	}
	
	public boolean isEmpty()
	{
		return this.blocks.isEmpty();
	}

	public ArrayList<BuyerOrderSecondIndexV23> getBlocks() {
		return blocks;
	}

	public void setBlocks(ArrayList<BuyerOrderSecondIndexV23> blocks) {
		this.blocks = blocks;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public BuyerOrderSecondIndexV23 find(String buyerid)
	{
		for(BuyerOrderSecondIndexV23 map : blocks)
		{
			if(map.getBuyerid().equals(buyerid)) return map;
		}
		return null;
	}
	
}
