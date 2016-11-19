package com.alibaba.middleware.race.test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.model.Row;



public class FileIOTest {
	
	private ArrayList<File> orderFileList;
	
	private String storeRootPath = RaceConfig.StoreRootPath;
	
	@org.junit.Test
	public void test() throws Exception
	{
		String str = "/disk1/orders/order.0.0";
		System.out.println(str.split("/")[1]);
	}
	
	@org.junit.Test
	public void test2() throws Exception
	{
		long startTime = System.currentTimeMillis();
		HashMap<Integer,BufferedOutputStream> map = new HashMap<Integer, BufferedOutputStream>();
		String rootPath = "../testFile/";
		int count = 200000;
		for(int i=0;i<count;i++)
		{
			System.out.println(i);
			File f = new File(rootPath+"tmp"+i);
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f),4096);
			map.put(i, bos);
		}
		
		
		for(int i=0;i<count;i++)
		{
			System.out.println(i);
			BufferedOutputStream bos = map.get(i);
			bos.flush();
			bos.close();
		}
		System.out.println("耗时："+(System.currentTimeMillis() - startTime));
		
	}
	
	@org.junit.Test
	public void test3() throws Exception
	{
		init();
		long startTime = System.currentTimeMillis();
		HashMap<Integer,BufferedOutputStream> map = new HashMap<Integer, BufferedOutputStream>();
		String rootPath = "../testFile/";
		int count = 60000;
		for(int i=0;i<count;i++)
		{
			System.out.println(i);
			File f = new File(rootPath+"tmp"+i);
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f),4096);
			map.put(i, bos);
		}
		System.out.println("创建"+count+"个文件消耗时间："+(System.currentTimeMillis()-startTime));
		
		System.out.println("创建文件完成，开始解析和写入");
		startTime = System.currentTimeMillis();
		byte[] tInByte = "\t".getBytes();
		for(File f : orderFileList)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),4096);
			String line = br.readLine();
			while(line != null)
			{
				Row row = RaceUtil.createRowFromRowStr(line);
				long orderid = row.getKV("orderid").valueAsLong();
				String buyerid = row.getKV("buyerid").valueAsString();
				BufferedOutputStream bos = map.get(Math.abs(buyerid.hashCode() % count));
				bos.write(buyerid.getBytes());
				bos.write(tInByte);
				bos.write(RaceUtil.longToBytes(orderid));
				bos.write(tInByte);	
				line = br.readLine();
			}
		}
		for(int i=0;i<count;i++)
		{
			BufferedOutputStream bos = map.get(i);
			bos.flush();
			bos.close();
		}
		System.out.println("写入和关闭文件耗时："+(System.currentTimeMillis() - startTime));
		
	}
	
	/*
	 * 测分磁盘多线程跟单线程区别
	 */
	@org.junit.Test
	public void test4() throws Exception
	{
		ArrayList<String> folders = new ArrayList<String>();
		folders.add("/opt/newdisk/data/disk2/store/");
		folders.add("/home/contest/disk1/store/");
		ArrayList<String> orderFolders = new ArrayList<String>();
		orderFolders.add("/opt/newdisk/data/disk2/orderFiles");
		orderFolders.add("/home/contest/disk1/orderFiles/");
		System.out.println("folders");
		System.out.println(folders);
		System.out.println("orderFolders");
		System.out.println(orderFolders);
		for(String str : folders)
		{
			File f = new File(str);
			RaceUtil.deleteAll(f);
			RaceUtil.mkDir(f);
		}
		
		System.out.println("准备开始单线程处理");
		//单线程处理两个磁盘上的文件
		long start1 = System.currentTimeMillis();
		for(int i=0;i<2;i++)
		{
			String orderName = orderFolders.get(i);
			String outputFileStr = folders.get(i)+"output_single_thread";
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(outputFileStr)),4096);
			File[] orderFiles = new File(orderName).listFiles();
			for(File orderFile : orderFiles)
			{
				System.out.println(orderFile.getName());
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(orderFile)),4096);
				String line = br.readLine();
				while(line != null)
				{
					Row row = RaceUtil.createRowFromRowStr(line);
					String buyerid = row.getKV("buyerid").valueAsString();
					long orderid = row.getKV("orderid").valueAsLong();
					bw.write(buyerid+"\t"+orderid);
					bw.newLine();
					line = br.readLine();
				}
				br.close();
			}
			bw.flush();
			bw.close();
		}
		
		System.out.println("单线程处理两个磁盘数据时间："+(System.currentTimeMillis() - start1));
		
		ExecutorService executor = Executors.newFixedThreadPool(4);
		//每个磁盘一个线程
		long start2 = System.currentTimeMillis();
		final CountDownLatch latch = new CountDownLatch(orderFolders.size());
		for(int i=0;i<folders.size();i++)
		{
			final String outputName = folders.get(i)+"output_multi_thread";
			System.out.println(outputName);
			final String orderDir = orderFolders.get(i);
			executor.execute(new Runnable() 
			{
				@Override
				public void run() {
					try{
						System.out.println("thread id:"+Thread.currentThread().getId());
						File[] orderFiles = new File(orderDir).listFiles();
						BufferedWriter bw = new BufferedWriter(
								new OutputStreamWriter(new FileOutputStream(outputName)),4096);
						for(File orderFile : orderFiles)
						{
							System.out.println(orderFile.getName());
							BufferedReader br = new BufferedReader(
									new InputStreamReader(new FileInputStream(orderFile)),4096);
							String line = br.readLine();
							while(line != null)
							{
								Row row = RaceUtil.createRowFromRowStr(line);
								String buyerid = row.getKV("buyerid").valueAsString();
								long orderid = row.getKV("orderid").valueAsLong();
								bw.write(buyerid+"\t"+orderid);
								bw.newLine();
								line = br.readLine();
							}
							br.close();
						}
						bw.flush();
						bw.close();
					}catch(Exception e){
						e.printStackTrace();
					}finally{
						latch.countDown();
					}
				}
			});
		}
		
		latch.await();
		System.out.println("两个线程处理两个磁盘时间："+(System.currentTimeMillis() - start2));
		
	}
	
	/*
	 * 测多线程处理一个目录下面的文件与单线程处理的区别
	 */
	@org.junit.Test
	public void test5() throws Exception
	{
		
		ArrayList<String> orderFolders = new ArrayList<String>();
		orderFolders.add("/opt/newdisk/data/disk2/orderFiles");
	
		System.out.println("orderFolders");
		System.out.println(orderFolders);
		System.out.println("准备开始单线程处理");
		//单线程处理两个磁盘上的文件
		long start1 = System.currentTimeMillis();
		String orderName = orderFolders.get(0);
		final ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<Long>();
		final File[] orderFiles = new File(orderName).listFiles();
		for(File orderFile : orderFiles)
		{
			System.out.println(orderFile.getName());
			BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(orderFile)),4096);
			String line = br.readLine();
			while(line != null)
			{
				Row row = RaceUtil.createRowFromRowStr(line);
				String buyerid = row.getKV("buyerid").valueAsString();
				long orderid = row.getKV("orderid").valueAsLong();
				queue.add(orderid);
				line = br.readLine();
			}
			br.close();
		}
		System.out.println("单线程数据时间："+(System.currentTimeMillis() - start1));
		
		ExecutorService executor = Executors.newFixedThreadPool(10);
		//每个磁盘一个线程
		long start2 = System.currentTimeMillis();
		final CountDownLatch latch = new CountDownLatch(orderFiles.length);
		for(int i=0;i<orderFiles.length;i++)
		{
			final File f = orderFiles[i];
			System.out.println(f.getName());
			executor.execute(new Runnable() {
				
				@Override
				public void run() {
					try{
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)),4096);
						String line = br.readLine();
						while(line != null)
						{
							Row row = RaceUtil.createRowFromRowStr(line);
							String buyerid = row.getKV("buyerid").valueAsString();
							long orderid = row.getKV("orderid").valueAsLong();
							queue.add(orderid);
							line = br.readLine();
						}
						br.close();
					}catch(Exception e){
						e.printStackTrace();
					}finally{
						latch.countDown();
					}
				}
			});
		}
		latch.await();
		System.out.println("两个线程处理两个磁盘时间："+(System.currentTimeMillis() - start2));
		
	}
	@org.junit.Test
	public void test6()
	{
		String str = "f15es";
		Long l = null;
		try{
			l = Long.parseLong(str);
		}catch(Exception e){
			
		}
		System.out.println((l == null));
	}
	@org.junit.Test
	public void test7() throws Exception
	{
		String line = "done:true	a_o_29364:2	buyerid:tp-aee5-d3cdd9f78c88	"
				+ "orderid:40817633951	a_o_639:1468383721933179	amount:15	"
				+ "createtime:8275202888	goodid:gd-9c88-8c33bbb659e8	a_o_23354:1468384524570650";
		Map<String,String> map = RaceUtil.createMapFrmoStr(line);
		System.out.println(map);
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
