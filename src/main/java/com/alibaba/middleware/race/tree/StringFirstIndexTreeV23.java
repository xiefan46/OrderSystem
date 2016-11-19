package com.alibaba.middleware.race.tree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.StringFirstIndex;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.TaskV3;

/*
 * 查找id为String的first index tree
 */
public class StringFirstIndexTreeV23 {
	
	//private final String secondIndexFileName;
	
	//private final String storeRootPath;
	
	private ConcurrentSkipListMap<String,StringFirstIndex> firstIndexTree
		=new ConcurrentSkipListMap<String, StringFirstIndex>(); //TODO：load factor和初始size可优化
	
	private RandomAccessFile[] secondIndexRafs;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	private ArrayList<DiskV23> disks;
	
	private String secondIndexFileName;
	
	private String storeRootPath;
	
	public StringFirstIndexTreeV23(String[] rafNames,ArrayList<DiskV23> disks,
			String storeRootPath, String secondIndexFileName) 
			throws Exception{
		this.secondIndexFileName = secondIndexFileName;
		this.disks = disks;
		this.storeRootPath = storeRootPath;
		constructFirstIndex();
		secondIndexRafs = new RandomAccessFile[disks.size()];
		for(int i=0;i<rafNames.length;i++)
		{
			System.out.println(rafNames[i]);
			secondIndexRafs[i] = new RandomAccessFile(rafNames[i], "r");
			System.out.println("rsf size:"+secondIndexRafs[i].length());
		}
	}
	
	public byte[] querySecondIndexDataById(String id)
	{
		int rafId = count.incrementAndGet() % disks.size();
		//System.out.println("string first index tree");
		Entry<String,StringFirstIndex> entry = firstIndexTree.floorEntry(id);
		if(entry == null) {
			System.out.println("发现id超出范围");
			return null;//id不在有效范围
		}
		StringFirstIndex firstIndex = entry.getValue();
		//System.out.println("first index id："+firstIndex.getId()+" offset:"+firstIndex.getOffset()+" length:"+firstIndex.getLength());
		TaskV3 task = new TaskV3(firstIndex.getOffset(), firstIndex.getLength(), secondIndexRafs[rafId]);
		//System.out.println("提交二级索引查询");
		disks.get(rafId).getIoThread().submitTask(task);
		byte[] block = task.getIOResult();
		//System.out.println("拿到二级索引数据");
		return block;
	}
	
	private void constructFirstIndex() throws Exception
	{
		File secondIndexFile = new File(storeRootPath + secondIndexFileName);
		if(!secondIndexFile.exists()){
			throw new Exception("二级索引文件不存在");
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(
				new FileInputStream(secondIndexFile)),4096);
		String line = br.readLine();
		long curOffset = 0;
		while(line != null)
		{
			int length = line.getBytes().length + 1 + passLines(br);
			String strs[] = line.split("\t");
			firstIndexTree.put(strs[0], new StringFirstIndex(strs[0], curOffset,length));
			curOffset += length;
			line = br.readLine();
		}
		br.close();
	}
	
	private int passLines(BufferedReader br) throws Exception
	{
		int length = 0;
		String line2 = null;
		for(int i=0;i<StringFirstIndex.BLOCK_SIZE-1;i++)
		{
			line2 = br.readLine();
			if(line2 == null) break;
			length = length + line2.getBytes().length + 1;
			
		}
		return length;
	}

}
