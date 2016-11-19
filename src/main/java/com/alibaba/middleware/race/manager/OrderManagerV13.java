package com.alibaba.middleware.race.manager;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.OrderSecondIndexV13;
import com.alibaba.middleware.race.model.Row;
import com.alibaba.middleware.race.sort.Sorter;

public class OrderManagerV13 
{
	private ArrayList<File> sortedOrderSmallFile = new ArrayList<File>();
	
	private static final int MAX_FILE_SIZE = 10*1024*1024;
	
	private ArrayList<OrderSecondIndexV13> unsortedOrderSecondIndex = new ArrayList<OrderSecondIndexV13>();
	
	private int count;
	
	private String storeRootPath;
	
	private int curTempId = 0;
	
	
	public OrderManagerV13(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void handle(long orderid,long offset) throws Exception
	{
		this.unsortedOrderSecondIndex.add(new OrderSecondIndexV13(orderid,offset));
		count += 24;
		if(count >= MAX_FILE_SIZE){
			sortAndUpdate();
		}
	}
	
	public void finish() throws Exception
	{
		if(count != 0){
			sortAndUpdate();
		}
	}
	
	private void sortAndUpdate() throws Exception
	{
		File f = sortAndSaveSecondIndexs(this.unsortedOrderSecondIndex, curTempId);
		this.sortedOrderSmallFile.add(f);
		this.count = 0;
		this.curTempId++;
		this.unsortedOrderSecondIndex.clear();
	}
	
	private File sortAndSaveSecondIndexs(ArrayList<OrderSecondIndexV13> secondIndexsList,int tmpFileID) throws Exception
	{
		//OrderSecondIndex[] secondIndexsArary = getArrayByArrayList(secondIndexsList);
		//Sorter.quicksort(secondIndexsArary);
		//Arrays.sort(secondIndexsArary);
		Collections.sort(secondIndexsList);
		File secondIndexTempFile = new File(storeRootPath+"sortedSecondIndexFiles_"+ tmpFileID);
		if(!secondIndexTempFile.exists())
			secondIndexTempFile.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(secondIndexTempFile),4096);
		for(OrderSecondIndexV13 osi : secondIndexsList)
		{
			writeOrderSecondIndex(bos, osi);
		}
		bos.flush();
		bos.close();
		
		return secondIndexTempFile;
	}
	
	private void writeOrderSecondIndex(BufferedOutputStream bos,OrderSecondIndexV13 osi) throws Exception
	{
		bos.write(RaceUtil.longToBytes(osi.getId()));
		bos.write(RaceUtil.longToBytes(osi.getOffset()));
	}
	

	public ArrayList<File> getSortedOrderSmallFile() {
		return sortedOrderSmallFile;
	}

	
	
	
	
}
