package com.alibaba.middleware.race.index.model;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import com.alibaba.middleware.race.RaceUtil;

/*
 * 需要支持long类型id和String类型id
 * 二级索引放于磁盘上
 * TODO：可以做缓存
 */
public class StringSecondIndexV13 implements Comparable<StringSecondIndexV13>{
	
	private  String id; 
	
	private long offset; //offset一定要用long

	
	public StringSecondIndexV13(String id,long offset)
	{
		this.id = id;
		this.offset = offset;
	}
	
	public StringSecondIndexV13(String line)
	{
		//System.out.println(line);
		String strs[] = line.split("\t");
		this.id = strs[0];
		this.offset = Long.parseLong(strs[1]);
	}
	
	//TODO:暂时全部用字符编码，后面可优化,secondIndex之间用\n隔开,字段之间用\t隔开
	public StringSecondIndexV13(byte[] secondIndexInByte)
	{
		String line = new String(secondIndexInByte);
		String strs[] = line.split("\t");
		this.id = strs[0];
		this.offset = Long.parseLong(strs[1]);
	}
	
	//格式：id fileId offset length id fileId offset length
	//TODO:目前采用的是顺序查找，可以优化为二分查找
	public static StringSecondIndexV13 getSecondIndex(byte[] block,String id) throws Exception
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(block)),4096);
		String line = br.readLine();
		while(line != null)
		{
			//System.out.println("还原到数据："+ line);
			StringSecondIndexV13 index = new StringSecondIndexV13(line);
			if(index.getId().equals(id)){
				return index;
			}
			line = br.readLine();
		}
		return null;
	}
	
	public static StringSecondIndexV13 createSecondIndex(byte[] secondIndexInBytes)
	{
		return new StringSecondIndexV13(secondIndexInBytes);
	}
	

	public String getId() {
		return id;
	}


	public long getOffset() {
		return offset;
	}


	
	@Override
	public int compareTo(StringSecondIndexV13 o) {
		return this.id.compareTo(o.id);
	}
	
	@Override
	public String toString()
	{
		return this.id + "\t" + this.offset;
	}
	
	
	
	
}
