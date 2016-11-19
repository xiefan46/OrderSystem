package com.alibaba.middleware.race.manager;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.BuyerOrderEntryV23;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.OrderposTime;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.model.Row;
import com.alibaba.middleware.race.sort.Sorter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class BuyerOrderManagerV23 {
	private ArrayList<File> sortedBuyerOrderSmallFile = new ArrayList<File>();
	
	private static final int MAX_FILE_SIZE = 10*1024*1024;
	
	private ArrayList<BuyerOrderEntryV23> unsortedBuyerOrderSecondIndex = new ArrayList<BuyerOrderEntryV23>();
	
	private int count;
	
	private String storeRootPath;
	
	private int tempFileId = 0;
	
	public BuyerOrderManagerV23(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void handle(String buyerid,long pos,long createtime) throws Exception
	{
		//long startTime = System.currentTimeMillis();
		BuyerOrderEntryV23 entry = new BuyerOrderEntryV23(buyerid,pos,createtime);
		this.unsortedBuyerOrderSecondIndex.add(entry);
		this.count += (buyerid.getBytes().length + 16);
		if(this.count >= MAX_FILE_SIZE){
			sortAndUpdate();
		}
		//handleOrderTime.addAndGet(System.currentTimeMillis() - startTime);
	}
	
	public void finish() throws Exception
	{
		if(count != 0){
			sortAndUpdate();
		}
	}
	
	private void sortAndUpdate() throws Exception
	{
		File f = sortAndSaveSecondIndexs(this.unsortedBuyerOrderSecondIndex, tempFileId);
		this.sortedBuyerOrderSmallFile.add(f);
		this.count = 0;
		this.tempFileId++;
		this.unsortedBuyerOrderSecondIndex.clear();
	}
	
	private File sortAndSaveSecondIndexs(ArrayList<BuyerOrderEntryV23> secondIndexsList,int tmpFileID) throws Exception
	{
		//BuyerOrderEntry[] secondIndexsArary = getArrayByArrayList(secondIndexsList);
		//Sorter.quicksort(secondIndexsArary);
		//Arrays.sort(secondIndexsArary);
		Collections.sort(secondIndexsList);
		File secondIndexTempFile = new File(storeRootPath+"buyerOrderSortedSecondIndexFiles_"+ tmpFileID);
		if(!secondIndexTempFile.exists())
			secondIndexTempFile.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(secondIndexTempFile),4096);
		ByteArrayOutputStream baos = new ByteArrayOutputStream(MAX_FILE_SIZE);
		Kryo kryo = KryoUtil.borrow();
		Output output = new Output(baos);
		for(BuyerOrderEntryV23 entry : secondIndexsList)
		{
			kryo.writeObject(output, entry);
		}
		output.flush();
		output.close();
		KryoUtil.release(kryo);
		bos.write(baos.toByteArray());
		bos.flush();
		bos.close();
		return secondIndexTempFile;
	}
	
	

	public ArrayList<File> getSortedBuyerOrderSmallFile() {
		return sortedBuyerOrderSmallFile;
	}
	
	
	
	
}
