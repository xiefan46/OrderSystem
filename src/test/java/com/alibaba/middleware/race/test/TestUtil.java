package com.alibaba.middleware.race.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.config.RaceConfig;

public class TestUtil {
	public static OrderSystem constructOs() throws Exception
	{
		File orderFileDir = new File(RaceConfig.OrderInfoPath);
		File buyerFileDir = new File(RaceConfig.BuyerInfoPath);
		File goodFileDir = new File(RaceConfig.GoodInfoPath);
		System.out.println(orderFileDir);
		System.out.println(buyerFileDir);
		System.out.println(goodFileDir);
		List<String> orderFileNames = new ArrayList<String>();
		List<String> buyerFileNames = new ArrayList<String>();
		List<String> goodFileNames = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();
		for(File f:orderFileDir.listFiles())
		{
			orderFileNames.add(RaceConfig.OrderInfoPath+f.getName());
		}
		for(File f:buyerFileDir.listFiles())
		{
			buyerFileNames.add(RaceConfig.BuyerInfoPath+f.getName());
		}
		for(File f:goodFileDir.listFiles())
		{
			goodFileNames.add(RaceConfig.GoodInfoPath+f.getName());
		}
		storeFolders.add(RaceConfig.StoreRootPath);
		OrderSystem os = new OrderSystemImpl();
		os.construct(orderFileNames, buyerFileNames, 
				goodFileNames, storeFolders);
		return os;
	}
	
	
}
