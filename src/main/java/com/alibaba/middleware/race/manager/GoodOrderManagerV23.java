package com.alibaba.middleware.race.manager;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.GoodOrderEntryV23;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.model.Row;
import com.alibaba.middleware.race.sort.Sorter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class GoodOrderManagerV23 {
	
	private ArrayList<File> sortedGoodOrderSmallFile = new ArrayList<File>();
	
	private static final int MAX_FILE_SIZE = 10*1024*1024;
	
	private ArrayList<GoodOrderEntryV23> unsortedGoodOrderSecondIndex = new ArrayList<GoodOrderEntryV23>();
	
	private int count;
	
	private String storeRootPath;
	
	private int tempFileId = 0;
	
	public GoodOrderManagerV23(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void handleOrder(String goodid,long pos) throws Exception
	{
		
		GoodOrderEntryV23 entry = new GoodOrderEntryV23(goodid,pos);
		this.unsortedGoodOrderSecondIndex.add(entry);
		this.count += (goodid.getBytes().length + 8);
		if(this.count >= MAX_FILE_SIZE){
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
		File f = sortAndSaveSecondIndexs(this.unsortedGoodOrderSecondIndex, tempFileId);
		this.sortedGoodOrderSmallFile.add(f);
		this.count = 0;
		this.tempFileId++;
		this.unsortedGoodOrderSecondIndex.clear();
	}
	
	private File sortAndSaveSecondIndexs(ArrayList<GoodOrderEntryV23> secondIndexsList,int tmpFileID) throws Exception
	{
		//GoodOrderEntry[] secondIndexsArary = getArrayByArrayList(secondIndexsList);
		//Sorter.quicksort(secondIndexsArary);
		//Arrays.sort(secondIndexsArary);
		Collections.sort(secondIndexsList);
		File secondIndexTempFile = new File(storeRootPath+"goodOrderSortedSecondIndexFiles_"+ tmpFileID);
		if(!secondIndexTempFile.exists())
			secondIndexTempFile.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(secondIndexTempFile),4096);
		ByteArrayOutputStream baos = new ByteArrayOutputStream(MAX_FILE_SIZE);
		Kryo kryo = KryoUtil.borrow();
		Output output = new Output(baos);
		for(GoodOrderEntryV23 entry : secondIndexsList)
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
	
	
	

	public ArrayList<File> getSortedGoodOrderSmallFile() {
		return sortedGoodOrderSmallFile;
	}
	
	
	
	
}
