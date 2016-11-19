package com.alibaba.middleware.race.test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.model.Row;

public class IndexTimeTest {
	
	private ArrayList<File> orderFileList;
	
	private String storeRootPath = RaceConfig.StoreRootPath;
	
	@org.junit.Test
	public void testAll() throws Exception
	{
		 init();
		 testReadAndParseTimeBigBuffer();
		 testReadBigBuffer();
		 testReadBigBufferAndWrite();
	}
	
	//测试读一遍订单文件并且解析的耗时
	@org.junit.Test
	public void testReadAndParseTime() throws Exception
	{
		//init();
		long startTime = System.currentTimeMillis();
		for(File f : orderFileList)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),4096);
			String line = br.readLine();
			while(line != null)
			{
				Row row = RaceUtil.createRowFromRowStr(line);
				line = br.readLine();
			}
			br.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("读一遍订单文件并且解析的耗时:"+(endTime-startTime)/1000+"秒");
	}
	
	@org.junit.Test
	public void testReadAndParseTimeBigBuffer() throws Exception
	{
		//init();
		long startTime = System.currentTimeMillis();
		for(File f : orderFileList)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),10*4096);
			String line = br.readLine();
			while(line != null)
			{
				Row row = RaceUtil.createRowFromRowStr(line);
				line = br.readLine();
			}
			br.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("将buffer调大十倍,读一遍订单文件并且解析的耗时:"+(endTime-startTime)/1000+"秒");
	}
	
	@org.junit.Test
	public void testReadBigBuffer() throws Exception
	{
		//init();
		long startTime = System.currentTimeMillis();
		for(File f : orderFileList)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),10*4096);
			String line = br.readLine();
			while(line != null)
			{
				line = br.readLine();
			}
			br.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("将buffer调大十倍,读一遍订单文件不做解析耗时:"+(endTime-startTime)/1000+"秒");
	}
	
	@org.junit.Test
	public void testReadBigBufferAndWrite() throws Exception
	{
		//init();
		long startTime = System.currentTimeMillis();
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output_order.log")),10*4096);
		for(File f : orderFileList)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),10*4096);
			String line = br.readLine();
			while(line != null)
			{
				Row r = RaceUtil.createRowFromRowStr(line);
				bw.write(r.toString());
				bw.newLine();
				line = br.readLine();
			}
			br.close();
		}
		bw.flush();
		bw.close();
		long endTime = System.currentTimeMillis();
		System.out.println("将buffer调大十倍,读一遍订单文件解析完再写回去耗时:"+(endTime-startTime)/1000+"秒");
	}
	
	
	
	
	
	
	private void init()
	{
		File orderFileDir = new File(RaceConfig.OrderInfoPath);
		List<String> orderFileNames = new ArrayList<String>();
		for(File f:orderFileDir.listFiles())
		{
			orderFileNames.add(RaceConfig.OrderInfoPath+f.getName());
		}
		this.orderFileList = RaceUtil.getFilesList(orderFileNames);
	}
}
