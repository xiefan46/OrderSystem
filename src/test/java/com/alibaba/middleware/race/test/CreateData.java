package com.alibaba.middleware.race.test;

import java.io.File;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.createdata.CreateDataUtil;

public class CreateData {
	public static void createData(int iter,int idCount)
	 {
		 String rootPath = "/home/contest/xiefan/project/middlewareStore100/";
		 String buyerPath = rootPath + "buyerFiles/";
		 String goodPath = rootPath + "goodFiles/";
		 String orderPath = rootPath+"orderFiles/";
		 RaceUtil.deleteAll(new File(rootPath));
		 RaceUtil.deleteAll(new File(buyerPath));
		 RaceUtil.deleteAll(new File(goodPath));
		 RaceUtil.deleteAll(new File(orderPath));
		 RaceUtil.mkDir(new File(buyerPath));
		 RaceUtil.mkDir(new File(goodPath));
		 RaceUtil.mkDir(new File(orderPath));
		 for(int i=0;i<iter;i++) //一次iter生成IdMaxNumInMemory个goodid和buyerid，并生成10-20倍的订单
		 {
			 String[] buyerIds = new String[idCount];
			 String[] goodIds = new String[idCount];
			 for(int j=0;j<idCount;j++)
			 {
				buyerIds[j] = CreateDataUtil.getBuyerId();
				goodIds[j] = CreateDataUtil.getGoodId();
			 }
			 String buyerFileName = buyerPath  + RaceConfig.BuyerFilePrefix + i;
			 String goodFileName = goodPath + RaceConfig.GoodFilePrefix + i;
			 
			 CreateDataUtil.genAllBuyerRecord(buyerFileName,buyerIds); //生成buyer文件
			 CreateDataUtil.genAllGoodRecord(goodFileName,goodIds); //生成good文件
			 //生成order文件(总大小为另外两个文件的25-50倍)
			 for(int j=0;j<5;j++)
			 {
				 String orderFileName = orderPath + RaceConfig.OrderFilePrefix + i + "_" +j;
				 int orderNumScale = 5 + (int)(Math.random()*5); //order数量为good和buyer的5-10倍
				 CreateDataUtil.genAllOrderRecord(orderFileName,buyerIds,goodIds,orderNumScale*idCount);
			 }
			 System.out.println("iter : "+i);
			
		 }
	 }
	
	@org.junit.Test
	public void create10GData(){
		createData(10, 120000);
	}
	@org.junit.Test
	public void create100GData(){
		createData(10, 1200000);
	}
}
