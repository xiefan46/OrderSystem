package com.alibaba.middleware.race.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.config.RaceConfig;

public class TestInServer {
	@org.junit.Test
	public void testConstruct() throws Exception
	{
		System.out.println("开始生成自己实现的订单系统myos");
		long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long startMemInMB = startMem / (1024*1024);
		System.out.println("生成前内存占用：" + startMemInMB +" MB");
		String rootPath = "/home/contest/xiefan/project/middlewareStore/";
		String orderStr = rootPath+"orderFiles/";
		String buyerStr = rootPath+"buyerFiles/";
		String goodStr = rootPath+"goodFiles/";
		File orderFileDir = new File(orderStr);
		File buyerFileDir = new File(buyerStr);
		File goodFileDir = new File(goodStr);
		System.out.println(orderFileDir);
		System.out.println(buyerFileDir);
		System.out.println(goodFileDir);
		List<String> orderFileNames = new ArrayList<String>();
		List<String> buyerFileNames = new ArrayList<String>();
		List<String> goodFileNames = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();
		String[] storeFoldersDir = {rootPath+"store1/"};
		for(File f:orderFileDir.listFiles())
		{
			orderFileNames.add(orderStr+f.getName());
		}
		for(File f:buyerFileDir.listFiles())
		{
			buyerFileNames.add(buyerStr+f.getName());
		}
		for(File f:goodFileDir.listFiles())
		{
			goodFileNames.add(goodStr+f.getName());
		}
		storeFolders = Arrays.asList(storeFoldersDir);
		OrderSystem myos = new OrderSystemImpl();
		myos.construct(orderFileNames, buyerFileNames, 
				goodFileNames, storeFolders);
		System.out.println("construct,现在准备gc以后测内存");
		System.gc();
		Thread.currentThread().sleep(10000);
		long myosMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long myosMemInMb = myosMem / (1024*1024) - startMemInMB;
		System.out.println("myos生成完成，占用内存："+ myosMemInMb + " MB");
		
	}
	
	@org.junit.Test
	public void testConstruct100() throws Exception
	{
		System.out.println("开始生成自己实现的订单系统myos");
		long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long startMemInMB = startMem / (1024*1024);
		System.out.println("生成前内存占用：" + startMemInMB +" MB");
		String rootPath = "/home/contest/xiefan/project/middlewareStore100/";
		String orderStr = rootPath+"orderFiles/";
		String buyerStr = rootPath+"buyerFiles/";
		String goodStr = rootPath+"goodFiles/";
		File orderFileDir = new File(orderStr);
		File buyerFileDir = new File(buyerStr);
		File goodFileDir = new File(goodStr);
		System.out.println(orderFileDir);
		System.out.println(buyerFileDir);
		System.out.println(goodFileDir);
		List<String> orderFileNames = new ArrayList<String>();
		List<String> buyerFileNames = new ArrayList<String>();
		List<String> goodFileNames = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();
		String[] storeFoldersDir = {rootPath+"store1/"};
		for(File f:orderFileDir.listFiles())
		{
			orderFileNames.add(orderStr+f.getName());
		}
		for(File f:buyerFileDir.listFiles())
		{
			buyerFileNames.add(buyerStr+f.getName());
		}
		for(File f:goodFileDir.listFiles())
		{
			goodFileNames.add(goodStr+f.getName());
		}
		storeFolders = Arrays.asList(storeFoldersDir);
		OrderSystem myos = new OrderSystemImpl();
		myos.construct(orderFileNames, buyerFileNames, 
				goodFileNames, storeFolders);
		System.gc();
		Thread.currentThread().sleep(10000);
		long myosMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long myosMemInMb = myosMem / (1024*1024) - startMemInMB;
		System.out.println("myos生成完成，占用内存："+ myosMemInMb + " MB");
		
	}
}
