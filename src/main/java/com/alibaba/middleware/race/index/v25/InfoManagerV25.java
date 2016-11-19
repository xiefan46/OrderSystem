package com.alibaba.middleware.race.index.v25;

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

public class InfoManagerV25 {
	
private String storeRootPath;
	
	private String secondIndexFileName = "secondIndexFile";
	
	private File secondIndexFile;
	
	private BufferedWriter bw;
	
	private int diskId;
	
	private ArrayList<StringSecondIndexV13> unsortedSecondIndex = new ArrayList<StringSecondIndexV13>();
	
	private static final int MAX_FILE_SIZE = 50 * 1024 * 1024;
	
	private int count = 0;
	
	public InfoManagerV25(String storeRootPath) throws Exception
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
		secondIndexFile = new File(storeRootPath+secondIndexFileName);
		if(!secondIndexFile.exists())
			secondIndexFile.createNewFile();
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(secondIndexFile)),4096);
	}
	
	public void handle(String id,long offset) throws Exception
	{
		StringSecondIndexV13 index = new StringSecondIndexV13(id, offset);
		this.unsortedSecondIndex.add(index);
		count += index.toString().getBytes().length;
		if(count > MAX_FILE_SIZE){
			update();
		}
	}
	
	public void finish() throws Exception
	{
		if(this.count != 0) {
			update();
		}
		bw.flush();
		bw.close();
	}
	
	private void update() throws Exception
	{
		saveSecondIndex(this.unsortedSecondIndex);
		this.count = 0;
		this.unsortedSecondIndex.clear();
	}
	
	private void saveSecondIndex(ArrayList<StringSecondIndexV13> secondIndexsList) throws Exception
	{
		
		for(StringSecondIndexV13 ssi : secondIndexsList)
		{
			writeSecondIndex(bw, ssi);
		}
		
	}
	
	private void writeSecondIndex(BufferedWriter bw ,StringSecondIndexV13 ssi) throws Exception
	{
		bw.write(ssi.toString());
		bw.write("\n");
	}

	public File getSecondIndexFile() {
		return secondIndexFile;
	}

	
	
	
}
