package com.alibaba.middleware.race.index.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.middleware.race.RaceUtil;

/*
 * 一级索引放于内存中
 */
public class StringFirstIndex
{
	public static final int BLOCK_SIZE = 16; //一个一级索引对应约16条二级索引
	
	private String id; //以每个二级索引中最小的id作为一级索引的id
	
	private long offset;
	
	private int length;
	
	public StringFirstIndex(String id,long offset,int length){
		this.id = id;
		this.offset = offset;
		this.length = length;
	}
	
	
	public String getId() {
		return id;
	}

	public long getOffset() {
		return offset;
	}


	public int getLength() {
		return length;
	}
	
	
}
