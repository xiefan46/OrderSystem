package com.alibaba.middleware.race.index.model;

import java.util.Arrays;

import com.alibaba.middleware.race.RaceUtil;

/*
 * 需要支持long类型id和String类型id
 * 二级索引放于磁盘上
 * TODO：可以做缓存
 */
public class OrderSecondIndexV13 implements Comparable<OrderSecondIndexV13>{
	
	private long id; 
	
	private long offset; //offset一定要用long
	
	public OrderSecondIndexV13(long id,long offset)
	{
		this.id = id;
		this.offset = offset;
	}
	
	public OrderSecondIndexV13(byte[] secondIndexInBytes){
		//System.out.println("secondIndexBytes size:"+secondIndexInBytes.length);
		byte[] idInByte = Arrays.copyOfRange(secondIndexInBytes, 0, 8);
		byte[] offsetInByte = Arrays.copyOfRange(secondIndexInBytes,8,16);
		//System.out.println("offsetInByte size:"+offsetInByte.length);
		this.id = RaceUtil.bytesToLong(idInByte);
		this.offset = RaceUtil.bytesToLong(offsetInByte);
	}
	
	//格式：id fileId offset length id fileId offset length
	//TODO:目前采用的是顺序查找，可以优化为二分查找
	public static OrderSecondIndexV13 getSecondIndex(byte[] block,long id) throws Exception
	{
		int size = block.length;
		if(size % 16 != 0){
			throw new Exception("对secondIndex解码时发现格式错误");
		}
		int pointer = 0; //一个指向id的指针
		while(pointer < size)
		{
			byte[] idInByte = Arrays.copyOfRange(block, pointer, pointer+8);
			if(RaceUtil.bytesToLong(idInByte) == id){
				byte[] offsetInByte = Arrays.copyOfRange(block,pointer+8,pointer+16);
				return new OrderSecondIndexV13(id, RaceUtil.bytesToLong(offsetInByte));
			}
			pointer += 16;
		}
		return null;
	}
	
	public static OrderSecondIndexV13 createSecondIndex(byte[] secondIndexInBytes)
	{
		return new OrderSecondIndexV13(secondIndexInBytes);
	}
	
	


	public long getId() {
		return id;
	}



	public void setId(long id) {
		this.id = id;
	}



	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	

	@Override
	public int compareTo(OrderSecondIndexV13 o) {
		if(this.id < o.id) return -1;
		else if(this.id == o.id) return 0;
		return 1;
	}
	
	
	
	
	
}
