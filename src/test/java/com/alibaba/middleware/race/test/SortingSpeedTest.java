package com.alibaba.middleware.race.test;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Random;

import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.sort.Sorter;


public class SortingSpeedTest {
	//实际订单数目估计为5亿条
	public final static int orderNum = 10 * 1000 * 1000; //测试用1000万条
	
	public static Random rand = new Random(System.currentTimeMillis());
	
	@org.junit.Test
	public void testIdSortOnly()
	{
		int times = 3;
		long singleThreadTotalTime = 0;
		long multiThreadTotalTime = 0;
		for(int t=0;t<times;t++)
		{
			Long[] data = new Long[orderNum];
			Long[] data2 = new Long[orderNum];
			for(int i=0;i<orderNum;i++)
			{
				data[i] = rand.nextLong();
			}
			System.arraycopy(data, 0, data2, 0, orderNum);
			System.out.println("开始第"+(t+1)+"次排序");
			long startTime = System.currentTimeMillis();
			Arrays.sort(data);
			long endTime1 = System.currentTimeMillis();
			Sorter.quicksort(data2);
			long endTime2 = System.currentTimeMillis();
			assertArrayEquals(data, data2);
			System.out.println("第"+(t+1)+"次比较，单线程排序时间："+(endTime1-startTime)
					+" 多线程快排时间:"+(endTime2-endTime1));
			singleThreadTotalTime += (endTime1-startTime);
			multiThreadTotalTime += (endTime2-endTime1);
		}	
		System.out.println("单线程排序平均时间："+(singleThreadTotalTime / times));
		System.out.println("多线程快排平均时间："+(multiThreadTotalTime / times));
	}
	
	@org.junit.Test
	public void testSecondIndexSort()
	{
		int times = 3;
		long singleThreadTotalTime = 0;
		long multiThreadTotalTime = 0;
		for(int t=0;t<times;t++)
		{
			SecondIndex[] data = new SecondIndex[orderNum];
			SecondIndex[] data2 = new SecondIndex[orderNum];
			long startCreate = System.currentTimeMillis();
			for(int i=0;i<orderNum;i++)
			{
				long id = rand.nextLong();
				long offset = rand.nextLong();
				int length = rand.nextInt();
				data[i] = new SecondIndex(id,new StoreInfo(offset, length));
				data2[i] = new SecondIndex(id,new StoreInfo(offset, length));
				if(i%1000000 == 0) System.out.println(i); 
			}
			System.out.println("生成数据所需时间："+(System.currentTimeMillis() - startCreate));
			System.out.println("开始第"+(t+1)+"次排序");
			long startTime = System.currentTimeMillis();
			Arrays.sort(data);
			long endTime1 = System.currentTimeMillis();
			Sorter.quicksort(data2);
			long endTime2 = System.currentTimeMillis();
			if(!testEqual(data,data2))
				System.out.println("两种方法排序结果不同");
			System.out.println("第"+(t+1)+"次比较，单线程排序时间："+(endTime1-startTime)
					+" 多线程快排时间:"+(endTime2-endTime1));
			singleThreadTotalTime += (endTime1-startTime);
			multiThreadTotalTime += (endTime2-endTime1);
		}	
		System.out.println("单线程排序平均时间："+(singleThreadTotalTime / times));
		System.out.println("多线程快排平均时间："+(multiThreadTotalTime / times));
	}
	
	private boolean testEqual(SecondIndex[] index1,SecondIndex[] index2)
	{
		for(int i=0;i<index1.length;i++)
		{
			if(index1[i].compareTo(index2[i]) != 0) return false;
		}
		return true;
	}
	
	private class SecondIndex implements Comparable<SecondIndex>{
		private long orderid;
		
		private StoreInfo storeInfo;
		
		public SecondIndex(){
			
		}
		
		public SecondIndex(long orderId,StoreInfo storeInfo)
		{
			this.orderid = orderId;
			this.storeInfo = storeInfo;
		}
		
		@Override
		public int compareTo(SecondIndex o) {
			if(this.orderid < o.orderid) return -1;
			else if(this.orderid > o.orderid) return 1;
			return 0;
		}
		
		
		

		public long getOrderid() {
			return orderid;
		}

		public void setOrderid(long orderid) {
			this.orderid = orderid;
		}

		public StoreInfo getStoreInfo() {
			return storeInfo;
		}

		public void setStoreInfo(StoreInfo storeInfo) {
			this.storeInfo = storeInfo;
		}
		
		 
	}
}
