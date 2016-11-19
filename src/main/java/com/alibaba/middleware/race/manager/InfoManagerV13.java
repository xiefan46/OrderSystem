package com.alibaba.middleware.race.manager;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.StringSecondIndexV13;
import com.alibaba.middleware.race.model.Row;
import com.alibaba.middleware.race.sort.Sorter;

public class InfoManagerV13 {
	
	private String storeRootPath;
	
	private ArrayList<File> sortedSecondSmallFile = new ArrayList<File>();
	
	private ArrayList<StringSecondIndexV13> unsortedSecondIndex = new ArrayList<StringSecondIndexV13>();
	
	private String primaryKey;
	
	private static final int MAX_FILE_SIZE = 50 * 1024 * 1024;
	
	private int count = 0;
	
	private int curTempId = 0;
	
	public InfoManagerV13(String storeRootPath,String primaryKey)
	{
		this.storeRootPath = storeRootPath;
		this.primaryKey = primaryKey;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void handle(String id,long offset) throws Exception
	{
		StringSecondIndexV13 index = new StringSecondIndexV13(id, offset);
		this.unsortedSecondIndex.add(index);
		count += index.toString().getBytes().length;
		if(count > MAX_FILE_SIZE){
			sortAndUpdate();
		}
	}
	
	public void finish() throws Exception
	{
		if(this.count != 0) {
			sortAndUpdate();
		}
	}
	
	private void sortAndUpdate() throws Exception
	{
		File f = sortAndSaveSecondIndexs(this.unsortedSecondIndex, curTempId);
		this.sortedSecondSmallFile.add(f);
		this.count = 0;
		this.curTempId++;
		this.unsortedSecondIndex.clear();
	}
	
	private File sortAndSaveSecondIndexs(ArrayList<StringSecondIndexV13> secondIndexsList,int tmpFileID) throws Exception
	{
		StringSecondIndexV13[] secondIndexsArray = getArrayByArrayList(secondIndexsList);
		Sorter.quicksort(secondIndexsArray);
		//Arrays.sort(secondIndexsArray);
		File secondIndexTempFile = new File(storeRootPath+"sortedSecondIndexFiles_"+ tmpFileID);
		if(!secondIndexTempFile.exists())
			secondIndexTempFile.createNewFile();
		//BufferedOutputStream bos = new BufferedOutputStream();
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(secondIndexTempFile)),4096);
		for(StringSecondIndexV13 ssi : secondIndexsArray)
		{
			writeSecondIndex(bw, ssi);
		}
		bw.flush();
		bw.close();
		return secondIndexTempFile;
	}
	
	private void writeSecondIndex(BufferedWriter bw ,StringSecondIndexV13 ssi) throws Exception
	{
		bw.write(ssi.toString());
		bw.write("\n");
	}
	
	private StringSecondIndexV13[] getArrayByArrayList(ArrayList<StringSecondIndexV13> list)
	{
		int size = list.size();
		StringSecondIndexV13[] result = new StringSecondIndexV13[size];
		for(int i=0;i<size;i++)
		{
			result[i] = list.get(i);
		}
		return result;
	}

	public ArrayList<File> getSortedSecondSmallFile() {
		return sortedSecondSmallFile;
	}
	
	
	
}
