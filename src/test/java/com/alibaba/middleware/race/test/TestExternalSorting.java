package com.alibaba.middleware.race.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.junit.Test;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.sort.ExternalSorting;
import com.alibaba.middleware.race.sort.RowComparator;

public class TestExternalSorting {

	
	
	@Test 
	public void testCreateFiles()
	{
		try{
			RaceUtil.createAllTestFiles(5);
			System.out.println("生成数据成功");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Test
	public void testExternalSorting()
	{
		try{
			
			ExternalSorting.externalSort(RaceConfig.OrderInfoPath, Arrays.asList(new String[]{"orderid"}), 
					RaceConfig.OrderIndexPath, RaceConfig.OrderIndexFileName);
			ExternalSorting.externalSort(RaceConfig.GoodInfoPath, Arrays.asList(new String[]{"goodid"}), 
					RaceConfig.GoodIndexPath, RaceConfig.GoodIndexFileName);
			ExternalSorting.externalSort(RaceConfig.BuyerInfoPath, Arrays.asList(new String[]{"buyerid"}), 
					RaceConfig.BuyerIndexPath, RaceConfig.BuyerIndexFileName);
			System.out.println("外排序成功");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSortingRight()
	{
		
		try {
			File f = new File(RaceConfig.OrderIndexPath+RaceConfig.OrderIndexFileName);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			int count = 0;
			String line = br.readLine();
			String lineBefore = line;
			boolean flag = true;
			RowComparator rc = new RowComparator(Arrays.asList(new String[]{"orderid"}));
			while(line != null)
			{
				if(count++ < 10){
					System.out.println(line);
				}
				if(rc.compare(lineBefore, line) > 0) {
					flag = false;
					break;
				}
				lineBefore = line;
				line = br.readLine();
			}
			System.out.println("排序正确吗:"+flag);
			br.close();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

}
