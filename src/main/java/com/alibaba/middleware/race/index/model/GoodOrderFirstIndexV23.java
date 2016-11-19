package com.alibaba.middleware.race.index.model;

import java.util.ArrayList;
import java.util.Collections;

public class GoodOrderFirstIndexV23 {
	
	private static final int BLOCK_SIZE = 8;
	
	private ArrayList<GoodOrderSecondIndexV23> blocks = new ArrayList<GoodOrderSecondIndexV23>();
	
	private String id = null;
	
	public GoodOrderFirstIndexV23(){
		
	}
	
	public void putBlock(GoodOrderSecondIndexV23 block)
	{
		this.blocks.add(block);
		if(this.id == null) this.id = block.getGoodid();
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

	public ArrayList<GoodOrderSecondIndexV23> getBlocks() {
		return blocks;
	}

	public void setBlocks(ArrayList<GoodOrderSecondIndexV23> blocks) {
		this.blocks = blocks;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public GoodOrderSecondIndexV23 find(String goodid)
	{
		for(GoodOrderSecondIndexV23 map : blocks)
		{
			if(map.getGoodid().equals(goodid)) return map;
		}
		return null;
	}
	
}
