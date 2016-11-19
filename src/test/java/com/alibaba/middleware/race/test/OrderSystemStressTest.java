package com.alibaba.middleware.race.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.database.RightOrderSystem;
import com.alibaba.middleware.race.model.KV;
import com.alibaba.middleware.race.model.ResultImpl;
import com.alibaba.middleware.race.model.Row;

public 	class OrderSystemStressTest {
	
	private static ExecutorService executorService = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors()*2);
	
	private static Random rand = new Random(System.currentTimeMillis());
	
	private OrderSystem ros;
	
	private OrderSystem myos;
	
	private String testSetPath = RaceConfig.StoreRootPath+"testSet/";
	
	private String testSetOrderFilesDir = testSetPath + "orderFiles/";
	
	private String testSetBuyerFilesDir = testSetPath + "buyerFiles/";
	
	private String testSetGoddFilesDir = testSetPath + "goodFiles/";
	
	//用于构造测试序列
	private CopyOnWriteArrayList<Long> orderIds;
	
	private CopyOnWriteArrayList<String> buyerIds;
	
	private CopyOnWriteArrayList<String> goodIds;
	
	//压力测试相关参数
	private final static int THREAD_NUM = 15;
	//查询testLoop个循环，每个循环queryEveryLoop个query，共THREAD_NUM*testLoop*queryEveryLoop次查询
	private final static int testLoop = 100;
	private final static int queryEveryLoop = 10;
	
	@org.junit.Test
	public void getMemory()
	{
		//total-free就是目前被占用的内存，max是jvm支持的最大内存，这里为1.7G
		long totalMemory = Runtime.getRuntime().totalMemory();
		long freeMemory = Runtime.getRuntime().freeMemory();
		long maxMemory = Runtime.getRuntime().maxMemory();
		System.out.println("total:"+totalMemory);
		System.out.println("free:"+freeMemory);
		System.out.println("max:"+maxMemory);
	}
	
	@org.junit.Test
	public void testAll() throws Exception
	{
		getMemory();
		System.out.println("测试开始");
		System.out.println("开始读取测试集的所有id");
		this.orderIds = createLongIds(testSetOrderFilesDir, "orderid");
		this.buyerIds = createStringIds(testSetBuyerFilesDir, "buyerid");
		this.goodIds = createStringIds(testSetGoddFilesDir, "goodid");
		constructRightOrderSystem();
		constructMyOrderSystem();
		boolean basicOK = testBasicFunction(1000);
		boolean strangeCasesOk = testStrangeCases(1000);
		assertEquals(basicOK,true);
		assertEquals(strangeCasesOk,true);
		stressTest();
		
	}
	
	@org.junit.Test
	public void testConstructMyOrderSystem() throws Exception
	{
		constructMyOrderSystem();
	}
	
	
	@org.junit.Test
	public void testBasic() throws Exception
	{
		testInit();
		boolean basicOK = testBasicFunction(10);
		assertEquals(basicOK,true);
	}
	
	@org.junit.Test
	public void testStrange() throws Exception
	{
		testInit();
		boolean strangeCasesOk = testStrangeCases(3);
		assertEquals(strangeCasesOk,true);
	}
	
	@org.junit.Test
	public void testStress() throws Exception
	{
		testInit();
		stressTest();
	}
	
	@org.junit.Test
	public void testHotSpot() throws Exception
	{
		testInit();
		hotSpotTest();
	}
	
	@org.junit.Test
	public void tianchiTest() throws Exception
	{
		testInit();
		//1
		long orderId = 589555952;
		ArrayList<String> keys = new ArrayList<String>();
		keys.add("amount");
		Result myResult = myos.queryOrder(orderId, keys);
		Result rightResult = ros.queryOrder(orderId, keys);
		if(compareResult(rightResult, myResult)){
			System.out.println("比较成功");
			System.out.println("my result:"+myResult);
			System.out.println("right result:"+rightResult);
		}
		//2
		orderId = 590372877;
		myResult = myos.queryOrder(orderId, null);
		rightResult = ros.queryOrder(orderId, null);
		if(compareResult(rightResult, myResult)){
			System.out.println("比较成功");
			System.out.println("my result:"+myResult);
			System.out.println("right result:"+rightResult);
		}
		//3
		orderId = 612401553;
		keys.clear();
		keys.add("a_b_26525");
		myResult = myos.queryOrder(orderId, keys);
		rightResult = ros.queryOrder(orderId, keys);
		if(compareResult(rightResult, myResult)){
			System.out.println("比较成功");
			System.out.println("my result:"+myResult);
			System.out.println("right result:"+rightResult);
		}
		//4
		String buyerid = "ap-ab95-3e7e0ed47717";
		Iterator<Result> myResultIter = myos.queryOrdersByBuyer(Long.MIN_VALUE, Long.MAX_VALUE, buyerid);
		Iterator<Result> rightResultIter = ros.queryOrdersByBuyer(Long.MIN_VALUE, Long.MAX_VALUE, buyerid);
		if(compareIterator(rightResultIter, myResultIter)){
			System.out.println("比较成功");
			System.out.println("my result iter:"+myResultIter);
			System.out.println("right result iter:"+rightResultIter);
		}
	}
	
	private void testInit() throws Exception
	{
		System.out.println("测试开始");
		System.out.println("开始读取测试集的所有id");
		this.orderIds = createLongIds(testSetOrderFilesDir, "orderid");
		this.buyerIds = createStringIds(testSetBuyerFilesDir, "buyerid");
		this.goodIds = createStringIds(testSetGoddFilesDir, "goodid");
		constructRightOrderSystem();
		constructMyOrderSystem();
	}
	
	private void hotSpotTest() throws Exception
	{
		boolean ok = queryOrderHotSpotTest();
		assertEquals(true, ok);
		/*ok = queryOrdersByBuyerHotSpotTest();
		assertEquals(true, ok);
		ok = queryOrdersBySalerHotSpotTest();
		assertEquals(true, ok);
		ok = sumOrdersByGoodHotSpotTest();
		assertEquals(true, ok);*/
	}
	
	

	private boolean queryOrderHotSpotTest() throws Exception
	{
		System.out.println("开始进行queryOrder热点查询测试");
		final AtomicBoolean ok = new AtomicBoolean(true);
		final AtomicLong totalCostTime = new AtomicLong(0); //所有线程总耗时，包括查询时间和检查结果时间
		final AtomicLong myosCostTime = new AtomicLong(0);  //myos耗时
		final AtomicLong rosCostTime = new AtomicLong(0);  //ros耗时
		final AtomicLong queryCount = new AtomicLong(0);   //完成的查询数
		final CountDownLatch latch = new CountDownLatch(THREAD_NUM);
		int size = orderIds.size() / 100; //1%的订单会被反复查
		final CopyOnWriteArrayList<Long> hostSpotIdList = new CopyOnWriteArrayList<Long>();
		final String[] keySet = {"buyername","你来打我呀","buyerid","goodid","app_buyer_5_5"};
		//构造查询的key数组
		final CopyOnWriteArrayList<String> keys = new CopyOnWriteArrayList<String>();
		for(String key : keySet)
			keys.add(key);
		//构造热点id
		for(int i=0;i<1;i++)
		{
			long id = orderIds.get(rand.nextInt(orderIds.size()));
			for(int j=0;j<100;j++){
				hostSpotIdList.add(id);
			}
		}
		//混入一些非热点id
		for(int i=0;i<size;i++)
		{
			hostSpotIdList.add(orderIds.get(rand.nextInt(orderIds.size())));
		}
		Collections.shuffle(hostSpotIdList);
		System.out.println("构造查询id集合完成，开始测试");
		for(int thread_num=0;thread_num<THREAD_NUM;thread_num++)
		{
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					long startTime = System.currentTimeMillis();
					for(int i=0;i<testLoop;i++)
					{
						List<Result> rosResults = new ArrayList<Result>(queryEveryLoop);
						List<Result> myosResults = new ArrayList<Result>(queryEveryLoop);
						List<Long> queryIds = new ArrayList<Long>(queryEveryLoop); //查询序列
						for(int j=0;j<queryEveryLoop;j++)
							queryIds.add(hostSpotIdList.get(rand.nextInt(hostSpotIdList.size())));
						//开始查询ros，构造正确答案
						long rosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							rosResults.add(ros.queryOrder(queryIds.get(j), keys));
						}
						rosCostTime.addAndGet(System.currentTimeMillis() - rosStart);
						//开始查询myos
						long myosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							myosResults.add(myos.queryOrder(queryIds.get(j), keys));
						}
						myosCostTime.addAndGet(System.currentTimeMillis() - myosStart);
						//比对查询结果
						for(int j=0;j<queryEveryLoop;j++)
						{
							if(!compareResult(rosResults.get(j),myosResults.get(j))){
								ok.getAndSet(false);
								System.out.println("热点测试出现错误结果");
							}
						}
						if((i+1) % (testLoop/100) == 0){
							//System.out.println("目前正在进行queryOrder接口压力测试，目前第"+(i+1)+"轮testLoop");
						}	
					}
					queryCount.addAndGet(testLoop*queryEveryLoop);
					totalCostTime.addAndGet(System.currentTimeMillis() - startTime);
					latch.countDown();
				}
			});
		}
		latch.await();
		if(ok.get() == true){
			int totalQueryCount = THREAD_NUM * testLoop * queryEveryLoop;
			System.out.println("queryOrder热点测试成功，结果如下");
			System.out.println("测试的热点key集合:"+keys);
			System.out.println("测试总耗时："+(totalCostTime.get()/1000) +"秒");
			System.out.println("ros总耗时："+(rosCostTime.get() / 1000)+"秒");
			System.out.println("myos总耗时："+(myosCostTime.get() / 1000)+"秒");
			System.out.println("query总数："+totalQueryCount);
			System.out.println("ros系统TPS："+(totalQueryCount*1000/rosCostTime.get()));
			System.out.println("myos系统TPS："+(totalQueryCount*1000/myosCostTime.get()));
		}else{
			System.out.println("压力测试失败，结果错误");
		}
		return ok.get();
	}
	
	/*private boolean queryOrdersByBuyerHotSpotTest()
	{
		
	}
	
	private boolean queryOrdersBySalerHotSpotTest()
	{
		
	}
	
	private boolean sumOrdersByGoodHotSpotTest()
	{
		
	}*/
	
	
	private void stressTest() throws Exception
	{
		System.out.println("开始进行压力测试");
		boolean queryOrderStressTestOk = queryOrderStressTest();
		assertEquals(queryOrderStressTestOk,true);
		boolean queryOrdersByBuyerStressTestOk = queryOrdersByBuyerStressTest();
		assertEquals(queryOrdersByBuyerStressTestOk,true);
		boolean queryOrdersBySalerStressOK = queryOrdersBySalerStressTest();
		assertEquals(queryOrdersBySalerStressOK,true);
		boolean sumOrdersByGoodOK = sumOrdersByGoodStressTest();
		assertEquals(sumOrdersByGoodOK,true);
		System.out.println("压力测试结束");
	}
	
	
	private boolean queryOrderStressTest() throws Exception
	{
		final AtomicBoolean ok = new AtomicBoolean(true);
		final AtomicLong totalCostTime = new AtomicLong(0); //所有线程总耗时，包括查询时间和检查结果时间
		final AtomicLong myosCostTime = new AtomicLong(0);  //myos耗时
		final AtomicLong rosCostTime = new AtomicLong(0);  //ros耗时
		final AtomicLong queryCount = new AtomicLong(0);   //完成的查询数
		final CountDownLatch latch = new CountDownLatch(THREAD_NUM);
		for(int thread_num=0;thread_num<THREAD_NUM;thread_num++)
		{
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					long startTime = System.currentTimeMillis();
					for(int i=0;i<testLoop;i++)
					{
						List<Result> rosResults = new ArrayList<Result>(queryEveryLoop);
						List<Result> myosResults = new ArrayList<Result>(queryEveryLoop);
						List<Long> queryIds = new ArrayList<Long>(queryEveryLoop); //查询序列
						for(int j=0;j<queryEveryLoop;j++)
							queryIds.add(orderIds.get(rand.nextInt(orderIds.size())));
						//开始查询ros，构造正确答案
						long rosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							rosResults.add(ros.queryOrder(queryIds.get(j), null));
						}
						rosCostTime.addAndGet(System.currentTimeMillis() - rosStart);
						//开始查询myos
						long myosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							myosResults.add(myos.queryOrder(queryIds.get(j), null));
						}
						myosCostTime.addAndGet(System.currentTimeMillis() - myosStart);
						//比对查询结果
						for(int j=0;j<queryEveryLoop;j++)
						{
							if(!compareResult(rosResults.get(j),myosResults.get(j))){
								ok.getAndSet(false);
								System.out.println("压测出现错误结果");
							}
						}
						if((i+1) % (testLoop/100) == 0){
							//System.out.println("目前正在进行queryOrder接口压力测试，目前第"+(i+1)+"轮testLoop");
						}	
					}
					queryCount.addAndGet(testLoop*queryEveryLoop);
					totalCostTime.addAndGet(System.currentTimeMillis() - startTime);
					latch.countDown();
				}
			});
		}
		latch.await();
		if(ok.get() == true){
			int totalQueryCount = THREAD_NUM * testLoop * queryEveryLoop;
			System.out.println("queryOrder压力测试成功，结果如下");
			System.out.println("测试总耗时："+(totalCostTime.get()/1000) +"秒");
			System.out.println("ros总耗时："+(rosCostTime.get() / 1000)+"秒");
			System.out.println("myos总耗时："+(myosCostTime.get() / 1000)+"秒");
			System.out.println("query总数："+totalQueryCount);
			System.out.println("ros系统TPS："+(totalQueryCount*1000/rosCostTime.get()));
			System.out.println("myos系统TPS："+(totalQueryCount*1000/myosCostTime.get()));
		}else{
			System.out.println("压力测试失败，结果错误");
		}
		return ok.get();
	}
	
	private boolean queryOrdersByBuyerStressTest() throws Exception
	{
		final AtomicBoolean ok = new AtomicBoolean(true);
		final AtomicLong totalCostTime = new AtomicLong(0); //所有线程总耗时，包括查询时间和检查结果时间
		final AtomicLong myosCostTime = new AtomicLong(0);  //myos耗时
		final AtomicLong rosCostTime = new AtomicLong(0);  //ros耗时
		final AtomicLong queryCount = new AtomicLong(0);   //完成的查询数
		final CountDownLatch latch = new CountDownLatch(THREAD_NUM);
		
		for(int thread_num=0;thread_num<THREAD_NUM;thread_num++)
		{
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					long startTime = System.currentTimeMillis();
					for(int i=0;i<testLoop;i++)
					{
						List<Iterator<Result>> rosResults = new ArrayList<Iterator<Result>>(queryEveryLoop);
						List<Iterator<Result>> myosResults = new ArrayList<Iterator<Result>>(queryEveryLoop);
						List<String> queryIds = new ArrayList<String>(queryEveryLoop); //查询序列
						for(int j=0;j<queryEveryLoop;j++)
							queryIds.add(buyerIds.get(rand.nextInt(buyerIds.size())));
						//开始查询ros，构造正确答案
						long rosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							rosResults.add(ros.queryOrdersByBuyer(Long.MIN_VALUE, Long.MAX_VALUE, queryIds.get(j)));
						}
						rosCostTime.addAndGet(System.currentTimeMillis() - rosStart);
						//开始查询myos
						long myosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							myosResults.add(myos.queryOrdersByBuyer(Long.MIN_VALUE, Long.MAX_VALUE, queryIds.get(j)));
						}
						myosCostTime.addAndGet(System.currentTimeMillis() - myosStart);
						//比对查询结果
						for(int j=0;j<queryEveryLoop;j++)
						{
							if(!compareIterator(rosResults.get(j), myosResults.get(j))){
								ok.getAndSet(false);
								System.out.println("压测出现错误结果");
							}
						}
						if((i+1) % (testLoop/100) == 0){
							//System.out.println("目前正在进行queryOrdersByBuyer接口压力测试，目前第"+(i+1)+"轮testLoop");
						}	
					}
					queryCount.addAndGet(testLoop*queryEveryLoop);
					totalCostTime.addAndGet(System.currentTimeMillis() - startTime);
					latch.countDown();
				}
			});
		}
		latch.await();
		if(ok.get() == true){
			int totalQueryCount = THREAD_NUM * testLoop * queryEveryLoop;
			System.out.println("queryOrdersByBuyer压力测试成功，结果如下");
			System.out.println("测试总耗时："+(totalCostTime.get()/1000) +"秒");
			System.out.println("ros总耗时："+(rosCostTime.get() / 1000)+"秒");
			System.out.println("myos总耗时："+(myosCostTime.get() / 1000)+"秒");
			System.out.println("query总数："+totalQueryCount);
			System.out.println("ros系统TPS："+(totalQueryCount*1000/rosCostTime.get()));
			System.out.println("myos系统TPS："+(totalQueryCount*1000/myosCostTime.get()));
		}else{
			System.out.println("压力测试失败，结果错误");
		}
		return ok.get();
	}
	
	private boolean queryOrdersBySalerStressTest() throws Exception
	{
		final AtomicBoolean ok = new AtomicBoolean(true);
		final AtomicLong totalCostTime = new AtomicLong(0); //所有线程总耗时，包括查询时间和检查结果时间
		final AtomicLong myosCostTime = new AtomicLong(0);  //myos耗时
		final AtomicLong rosCostTime = new AtomicLong(0);  //ros耗时
		final AtomicLong queryCount = new AtomicLong(0);   //完成的查询数
		final CountDownLatch latch = new CountDownLatch(THREAD_NUM);
		
		for(int thread_num=0;thread_num<THREAD_NUM;thread_num++)
		{
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					long startTime = System.currentTimeMillis();
					for(int i=0;i<testLoop;i++)
					{
						List<Iterator<Result>> rosResults = new ArrayList<Iterator<Result>>(queryEveryLoop);
						List<Iterator<Result>> myosResults = new ArrayList<Iterator<Result>>(queryEveryLoop);
						List<String> queryIds = new ArrayList<String>(queryEveryLoop); //查询序列
						for(int j=0;j<queryEveryLoop;j++)
							queryIds.add(goodIds.get(rand.nextInt(goodIds.size())));
						//开始查询ros，构造正确答案
						long rosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							rosResults.add(ros.queryOrdersBySaler("暂时不考虑salerid", queryIds.get(j), null));
						}
						rosCostTime.addAndGet(System.currentTimeMillis() - rosStart);
						//开始查询myos
						long myosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							myosResults.add(myos.queryOrdersBySaler("暂时不考虑salerid", queryIds.get(j), null));
						}
						myosCostTime.addAndGet(System.currentTimeMillis() - myosStart);
						//比对查询结果
						for(int j=0;j<queryEveryLoop;j++)
						{
							if(!compareIterator(rosResults.get(j), myosResults.get(j))){
								ok.getAndSet(false);
								System.out.println("压测出现错误结果");
							}
						}
						if((i+1) % (testLoop/100) == 0){
							//System.out.println("目前正在进行queryOrdersBySaler接口压力测试，目前第"+(i+1)+"轮testLoop");
						}	
					}
					queryCount.addAndGet(testLoop*queryEveryLoop);
					totalCostTime.addAndGet(System.currentTimeMillis() - startTime);
					latch.countDown();
				}
			});
		}
		latch.await();
		if(ok.get() == true){
			int totalQueryCount = THREAD_NUM * testLoop * queryEveryLoop;
			System.out.println("queryOrdersBySaler压力测试成功，结果如下");
			System.out.println("测试总耗时："+(totalCostTime.get()/1000) +"秒");
			System.out.println("ros总耗时："+(rosCostTime.get() / 1000)+"秒");
			System.out.println("myos总耗时："+(myosCostTime.get() / 1000)+"秒");
			System.out.println("query总数："+totalQueryCount);
			System.out.println("ros系统TPS："+(totalQueryCount*1000/rosCostTime.get()));
			System.out.println("myos系统TPS："+(totalQueryCount*1000/myosCostTime.get()));
		}else{
			System.out.println("压力测试失败，结果错误");
		}
		return ok.get();
	}

	
	private boolean sumOrdersByGoodStressTest() throws Exception
	{
		final AtomicBoolean ok = new AtomicBoolean(true);
		final AtomicLong totalCostTime = new AtomicLong(0); //所有线程总耗时，包括查询时间和检查结果时间
		final AtomicLong myosCostTime = new AtomicLong(0);  //myos耗时
		final AtomicLong rosCostTime = new AtomicLong(0);  //ros耗时
		final CountDownLatch latch = new CountDownLatch(THREAD_NUM);
		
		for(int thread_num=0;thread_num<THREAD_NUM;thread_num++)
		{
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					long startTime = System.currentTimeMillis();
					for(int i=0;i<testLoop;i++)
					{
						List<KeyValue> rosResults = new ArrayList<KeyValue>(queryEveryLoop);
						List<KeyValue> myosResults = new ArrayList<KeyValue>(queryEveryLoop);
						List<String> queryIds = new ArrayList<String>(queryEveryLoop); //查询序列
						for(int j=0;j<queryEveryLoop;j++)
							queryIds.add(goodIds.get(rand.nextInt(goodIds.size())));
						//开始查询ros，构造正确答案
						long rosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							rosResults.add(ros.sumOrdersByGood(queryIds.get(j), "amount"));
						}
						rosCostTime.addAndGet(System.currentTimeMillis() - rosStart);
						//开始查询myos
						long myosStart = System.currentTimeMillis();
						for(int j=0;j<queryEveryLoop;j++)
						{
							myosResults.add(myos.sumOrdersByGood(queryIds.get(j), "amount"));
						}
						myosCostTime.addAndGet(System.currentTimeMillis() - myosStart);
						//比对查询结果
						for(int j=0;j<queryEveryLoop;j++)
						{
							if(!compareKV(rosResults.get(j), myosResults.get(j))){
								ok.getAndSet(false);
								System.out.println("压测出现错误结果");
							}
						}
						if((i+1) % (testLoop/100) == 0){
							//System.out.println("目前正在进行sumOrdersByGood接口压力测试，目前第"+(i+1)+"轮testLoop");
						}	
					}
					totalCostTime.addAndGet(System.currentTimeMillis() - startTime);
					latch.countDown();
				}
			});
		}
		latch.await();
		if(ok.get() == true){
			int totalQueryCount = THREAD_NUM * testLoop * queryEveryLoop;
			System.out.println("sumOrdersByGood压力测试成功，结果如下");
			System.out.println("测试总耗时："+(totalCostTime.get()/1000) +"秒");
			System.out.println("ros总耗时："+(rosCostTime.get() / 1000)+"秒");
			System.out.println("myos总耗时："+(myosCostTime.get() / 1000)+"秒");
			System.out.println("query总数："+totalQueryCount);
			System.out.println("ros系统TPS："+(totalQueryCount*1000/rosCostTime.get()));
			System.out.println("myos系统TPS："+(totalQueryCount*1000/myosCostTime.get()));
		}else{
			System.out.println("压力测试失败，结果错误");
		}
		return ok.get();
	}

	//@org.junit.Test
	/*public void testQueryOrderByBuyer() throws Exception
	{
		//constructMyOrderSystem();
		constructRightOrderSystem();
		for(int i=0;i<100;i++){
			System.out.println("i:"+i);
			Iterator<Result> iter = ros.queryOrdersByBuyer(Long.MIN_VALUE, 
					Long.MAX_VALUE, "ap_298fccb8-29d3-4ce2-be70-6cd98bf7f155");
			while(iter.hasNext()){
				System.out.println(iter.next().orderId()+" ");
			}
		}
	}*/
	
	private boolean testBasicFunction(int testTimes)
	{
		System.out.println("测试主人所写的程序的基本功能");
		boolean ok = true;
		//测试第一个接口
		ok = ok && testQueryOrderBasic(testTimes);
		//测试第二个接口 
		//TODO:时间相同但是orderid顺序不同如何检测
		ok = ok && testQueryOrderByBuyerBasic(testTimes);
		//测试第三个接口
		ok = ok && testQueryOrderBySalerBasic(testTimes);
		//测试第四个接口
		ok = ok && testSumOrdersByGoodBasic(testTimes);
		System.out.println("基本功能测试结束，结果为"+ok);
		return ok;
	}
	
	private boolean testQueryOrderBasic(int testTimes)
	{
		boolean ok = true;
		System.out.println("测试第一个接口");
		for(int i=0;i<testTimes;i++)
		{
			long orderid = orderIds.get(rand.nextInt(orderIds.size()));
			//long orderid = 23566;
			//System.out.println("本次测试的orderid"+orderid);
			Result correctResult = ros.queryOrder(orderid, null);
			Result myResult = myos.queryOrder(orderid, null);
			if(!compareResult(correctResult, myResult))
			{
				System.out.println("第一个接口测试错误");
				ok = false;
			}
		}
		System.out.println("第一个接口测试结束");
		return ok;
	}
	
	private boolean testQueryOrderByBuyerBasic(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String buyerId = buyerIds.get(rand.nextInt(buyerIds.size()));
			//System.out.println("本次测试buyerid为："+buyerId);
			Iterator<Result> correctIter = ros.queryOrdersByBuyer(Long.MIN_VALUE, 
					Long.MAX_VALUE, buyerId);
			Iterator<Result> myIter = myos.queryOrdersByBuyer(Long.MIN_VALUE, 
					Long.MAX_VALUE, buyerId);
			//Iterator<Result> t1 = correctIter;
			//Iterator<Result> t2 = myIter;
			if(!compareIterator(correctIter, myIter)){
				System.out.println("第二个接口测试错误");
				System.out.println("本次测试的buyerid为"+buyerId);
				/*System.out.println("我的result如下：");
				while(t1.hasNext())
					System.out.println(t1.next());
				System.out.println("正确result如下");
				while(t2.hasNext())
					System.out.println(t2.next());*/
				ok = false;
			}
		}
		System.out.println("第二个接口测试结束");
		return ok;
	}
	
	private boolean testQueryOrderBySalerBasic(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String goodId = goodIds.get(rand.nextInt(goodIds.size()));
			//salerid暂时无用
			Iterator<Result> correctIter = ros.queryOrdersBySaler("123", goodId, null);
			Iterator<Result> myIter = myos.queryOrdersBySaler("123", goodId, null);
			if(!compareIterator(correctIter, myIter)){
				System.out.println("第三个接口测试错误");
				ok = false;
			}
		}
		System.out.println("第三个接口测试结束");
		return ok;
	}
	
	private boolean testSumOrdersByGoodBasic(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String goodId = goodIds.get(rand.nextInt(goodIds.size()));
			KeyValue correctKV = ros.sumOrdersByGood(goodId, "amount");
			KeyValue myKV = myos.sumOrdersByGood(goodId, "amount");
			if(!compareKV(correctKV, myKV)){
				System.out.println("第四个接口测试错误");
				ok = false;
			}
		}
		System.out.println("第四个接口测试结束");
		if(!ok){
			System.out.println("基础测试都过不了，呵呵");
		}
		return ok;
	}
	
	private boolean testStrangeCases(int testTimes)
	{
		System.out.println("开始测试各种异常情况");
		boolean ok = true;
		//int testTimes = orderIds.size() / 100000;
		ok = ok && testQueryOrderStrangeCases(testTimes);	
		ok = ok && testQueryOrdersByBuyerStrangeCases(testTimes);	
		ok = ok && testQueryOrdersBySalerStrangeCases(testTimes);
		ok = ok && testSumOrdersByGoodStrangeCases(testTimes);
		if(!ok){
			System.out.println("呵呵哒，各种异常");
		}else{
			System.out.println("通过异常测试");
		}
		return ok;
	}
	
	private boolean testQueryOrderStrangeCases(int testTimes)
	{
		boolean ok = true;
		//订单id不存在
		for(int i=0;i<testTimes;i++)
		{
			if(!compareResult(ros.queryOrder(0, null), myos.queryOrder(0, null))){
				ok = false;
				System.out.println("第一个接口无法通过订单id不存在的情况");
			}
				
		}
		
		
		//2.查询的key中所有字段都不存在
		for(int i=0;i<testTimes;i++)
		{
			long orderid = orderIds.get(rand.nextInt(orderIds.size()));
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			if(!compareResult(ros.queryOrder(orderid, keys), myos.queryOrder(orderid, keys))){
				System.out.println("第一个接口无法通过key中所有字段都不存在的情况");
				ok = false;
			}
				
		}
		//3.查询的key中有些存在有些不存在，且横跨三个订单
		for(int i=0;i<testTimes;i++)
		{
			long orderid = orderIds.get(rand.nextInt(orderIds.size()));
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			keys.add("orderid");
			keys.add("price");
			keys.add("goodname");
			keys.add("contactphone");
			keys.add("buyername");
			if(!compareResult(ros.queryOrder(orderid, keys), myos.queryOrder(orderid, keys)))
			{
				System.out.println("第一个接口无法通过有些字段存在，有些不存在的情况");
				ok = false;
			}
		}
		//key中只有orderid一个字段
		for(int i=0;i<testTimes;i++)
		{
			long orderid = orderIds.get(rand.nextInt(orderIds.size()));
			Collection<String> keys = new HashSet<String>();
			keys.add("orderid");
			if(!compareResult(ros.queryOrder(orderid, keys), myos.queryOrder(orderid, keys)))
			{
				System.out.println("第一个接口无法通过key为空容器的情况,且订单不存在的情况");
				ok = false;
			}
		}
		//key为空容器，且订单id不存在
		for(int i=0;i<testTimes;i++)
		{
			long orderid = 0;
			Collection<String> keys = new HashSet<String>();
			if(!compareResult(ros.queryOrder(orderid, keys), myos.queryOrder(orderid, keys)))
			{
				System.out.println("第一个接口无法通过key为空容器的情况,且订单不存在的情况");
				ok = false;
			}
		}
		
		
		System.out.println("第一个接口异常测试结束,结果为"+ok);
		return ok;
	}
	
	private boolean testQueryOrdersByBuyerStrangeCases(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String buyerid = buyerIds.get(rand.nextInt(buyerIds.size()));
			long startTime = Long.MAX_VALUE;
			long endTime = Long.MIN_VALUE;
			if(!compareIterator(ros.queryOrdersByBuyer(startTime, endTime, buyerid), 
					myos.queryOrdersByBuyer(startTime, endTime, buyerid))){
				System.out.println("第二个接口无法通过startTime大于endTime的情况");
				ok = false;
			}
		}
		//2 buyerid为null
		//System.out.println("测试buyerid为null的情况");
		for(int i=0;i<testTimes;i++)
		{
			String buyerid = null;
			long startTime = Long.MIN_VALUE;
			long endTime = Long.MAX_VALUE;
			if(!compareIterator(ros.queryOrdersByBuyer(startTime, endTime, buyerid), 
					myos.queryOrdersByBuyer(startTime, endTime, buyerid))){
				System.out.println("第二个接口无法通过buyerid为null的情况");
				ok = false;
			}
		}
		//3 buyerid为""
		//System.out.println("测试buyerid为空白字符串的情况");
		for(int i=0;i<testTimes;i++)
		{
			String buyerid = "";
			long startTime = Long.MIN_VALUE;
			long endTime = Long.MAX_VALUE;
			if(!compareIterator(ros.queryOrdersByBuyer(startTime, endTime, buyerid), 
					myos.queryOrdersByBuyer(startTime, endTime, buyerid))){
				System.out.println("第二个接口无法通过buyerid为空白字符串的情况");
				ok = false;
			}
		}
		//4 buyerid不存在
		//System.out.println("测试buyerid不存在的情况");
		for(int i=0;i<testTimes;i++)
		{
			String buyerid = "你来打我呀\n\t\fjiaejfia";
			long startTime = Long.MIN_VALUE;
			long endTime = Long.MAX_VALUE;
			if(!compareIterator(ros.queryOrdersByBuyer(startTime, endTime, buyerid), 
					myos.queryOrdersByBuyer(startTime, endTime, buyerid))){
				System.out.println("第二个接口无法通过buyerid不存在的情况");
				ok = false;
			}
		}
		System.out.println("第二个接口异常测试结束");
		return ok;
	}
	
	private boolean testQueryOrdersBySalerStrangeCases(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String goodid = goodIds.get(rand.nextInt(goodIds.size()));
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				ok = false;
				System.out.println("第三个接口无法通过查询key都不存在的情况");
			}
		}
		//2 查询的keys中有些字段存在有些不存在,其横跨三个订单
		for(int i=0;i<testTimes;i++)
		{
			String goodid = goodIds.get(rand.nextInt(goodIds.size()));
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			keys.add("orderid");
			keys.add("price");
			keys.add("goodname");
			keys.add("contactphone");
			keys.add("buyername");
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				System.out.println("第三个接口无法通过查询key有些字段存在有些不存在的情况");
				ok = false;
			}
		}
		//3 goodid不存在
		for(int i=0;i<testTimes;i++)
		{
			String goodid = "你来打我呀feasfa%%%";
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			keys.add("orderid");
			keys.add("price");
			keys.add("goodname");
			keys.add("contactphone");
			keys.add("buyername");
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				System.out.println("第三个接口无法通过goodid不存在的情况");
				ok = false;
			}
		}
		//4 goodid为null
		for(int i=0;i<testTimes;i++)
		{
			String goodid = null;
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			keys.add("orderid");
			keys.add("price");
			keys.add("goodname");
			keys.add("contactphone");
			keys.add("buyername");
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				System.out.println("第三个接口无法通过goodid为null的情况");
				ok = false;
			}
		}
		//5 goodid为""
		for(int i=0;i<testTimes;i++)
		{
			String goodid = "";
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			keys.add("");
			keys.add("&^*^&^*&687");
			keys.add(null);
			keys.add("null");
			keys.add("你来打我呀");
			keys.add("orderid");
			keys.add("price");
			keys.add("goodname");
			keys.add("contactphone");
			keys.add("buyername");
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				System.out.println("第三个接口无法通过goodid为空白字符串的情况");
				ok = false;
			}
		}
		//6 key是一个空容器
		for(int i=0;i<testTimes;i++)
		{
			String goodid = "jifeas";
			String salerid = "不考虑salerid";
			Collection<String> keys = new HashSet<String>();
			if(!compareIterator(ros.queryOrdersBySaler(salerid, goodid, keys), 
					myos.queryOrdersBySaler(salerid, goodid, keys))){
				System.out.println("第三个接口无法key为空容器的情况，且goodid不存在的情况");
				ok = false;
			}
		}
		System.out.println("第三个接口异常测试结束");
		return ok;
	}
	
	private boolean testSumOrdersByGoodStrangeCases(int testTimes)
	{
		boolean ok = true;
		for(int i=0;i<testTimes;i++)
		{
			String goodid = "你来打我呀";
			String salerid = "不考虑salerid";
			String key = "amount";
			if(!compareKV(ros.sumOrdersByGood(goodid, key), myos.sumOrdersByGood(goodid, key)))
			{
				ok = false;
				System.out.println("第四个接口测试失败，无法通过goodid不存在的情况");
			}
		}
		//2 goodid为null
		for(int i=0;i<testTimes;i++)
		{
			String goodid = null;
			String salerid = "不考虑salerid";
			String key = "amount";
			if(!compareKV(ros.sumOrdersByGood(goodid, key), myos.sumOrdersByGood(goodid, key)))
			{
				ok = false;
				System.out.println("第四个接口测试失败，无法通过goodid为null的情况");
			}
		}
		//3 key不存在
		for(int i=0;i<testTimes;i++)
		{
			String goodid = goodIds.get(rand.nextInt(goodIds.size()));
			String salerid = "不考虑salerid";
			String key = "你来打我呀fjeiajfoajefijo";
			if(!compareKV(ros.sumOrdersByGood(goodid, key), myos.sumOrdersByGood(goodid, key)))
			{
				ok = false;
				System.out.println("第四个接口测试失败，无法通过key不存在的情况");
			}
		}
		//4 key类型错误
		for(int i=0;i<testTimes;i++)
		{
			String goodid = goodIds.get(rand.nextInt(goodIds.size()));
			String salerid = "不考虑salerid";
			String key = "buyername";
			if(!compareKV(ros.sumOrdersByGood(goodid, key), myos.sumOrdersByGood(goodid, key)))
			{
				ok = false;
				System.out.println("第四个接口测试失败，无法通过key类型错误的情况");
			}
		}	
		System.out.println("第四个接口异常测试结束");
		return ok;
	}
	
	
	private CopyOnWriteArrayList<Long> createLongIds(String fileDir,String idName) throws Exception
	{
		CopyOnWriteArrayList<Long> ids = new CopyOnWriteArrayList<Long>();
		File dir = new File(fileDir);
		for(File f : dir.listFiles())
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line = br.readLine();
			while(line != null)
			{
				Row row = createRowFromRowStr(line);
				ids.add(row.get(idName).valueAsLong());
				line = br.readLine();
			}
			br.close();
		}
		return ids;
	}
	
	private CopyOnWriteArrayList<String> createStringIds(String fileDir,String idName) throws Exception
	{
		CopyOnWriteArrayList<String> ids = new CopyOnWriteArrayList<String>();
		File dir = new File(fileDir);
		for(File f : dir.listFiles())
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line = br.readLine();
			while(line != null)
			{
				Row row = createRowFromRowStr(line);
				ids.add(row.get(idName).valueAsString());
				line = br.readLine();
			}
			br.close();
		}
		return ids;
	}
	
	 private Row createRowFromRowStr(String rowStr) 
	  {
		    //System.out.println("rowStr:"+rowStr);
		    String[] kvs = rowStr.split("\t");
		    Row kvMap = new Row();
		    for (String rawkv : kvs) {
		      int p = rawkv.indexOf(':');
		      String key = rawkv.substring(0, p);
		      String value = rawkv.substring(p + 1);
		      if (key.length() == 0 || value.length() == 0) {
		        throw new RuntimeException("Bad data:" + rowStr);
		      }
		      KV kv = new KV(key, value);
		      kvMap.put(kv.key(), kv);
		    }
		    return kvMap;
	  }	 
	
	
	private void constructRightOrderSystem() throws Exception
	{
		System.out.println("开始生成检测程序ros");
		long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long startMemInMB = startMem / (1024*1024);
		System.out.println("生成前内存占用：" + startMemInMB +" MB");		
		List<String> testOrderFiles = new ArrayList<String>();
		List<String> testBuyerFiles = new ArrayList<String>();
		List<String> testGoodFiles = new ArrayList<String>();
		List<String> testStoreFolders = new ArrayList<String>();
		addAllFile(testSetOrderFilesDir, testOrderFiles);
		addAllFile(testSetBuyerFilesDir,testBuyerFiles);
		addAllFile(testSetGoddFilesDir, testGoodFiles);
		testStoreFolders.add("./");
		ros = new RightOrderSystem();
		ros.construct(testOrderFiles, testBuyerFiles, testGoodFiles, testStoreFolders);
		long rosMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		long rosMemInMb = rosMem / (1024*1024) - startMemInMB;
		System.out.println("检测程序ros生成完成，占用内存："+ rosMemInMb + " MB");
		
	}
	
	private void constructMyOrderSystem() throws Exception
	{
		System.out.println("开始生成自己实现的订单系统myos");
		//long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		//long startMemInMB = startMem / (1024*1024);
		//System.out.println("生成前内存占用：" + startMemInMB +" MB");
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
		storeFolders.add(RaceConfig.StoreRootPath+"store1/");
		storeFolders.add(RaceConfig.StoreRootPath+"store2/");
		storeFolders.add(RaceConfig.StoreRootPath+"store3/");
		myos = new OrderSystemImpl();
		myos.construct(orderFileNames, buyerFileNames, 
				goodFileNames, storeFolders);
		//System.gc();
		//Thread.currentThread().sleep(10000);
		//long myosMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		//long myosMemInMb = myosMem / (1024*1024) - startMemInMB;
		//System.out.println("myos生成完成，占用内存："+ myosMemInMb + " MB");
	}
	
	private void addAllFile(String dir,List<String> files)
	{
		File fileDir = new File(dir);
		for(File f:fileDir.listFiles()){
			files.add(dir+f.getName());
		}
	}
	
	private boolean compareIterator(Iterator<Result> iter1,Iterator<Result> iter2)
	{
		if(iter1 == null && iter2 == null) return true;
		if(iter1 == null && iter2 != null){
			System.out.println("iterator比较错误，正确答案为null。");
			return false;
		}
		if(iter1 != null && iter2 == null){
			System.out.println("iterator比较错误，我的答案为null，正确答案非null");
			return false;
		}
		while(iter1.hasNext() && iter2.hasNext())
		{
			Result correctResult = iter1.next();
			Result myResult = iter2.next();
			if(!compareResult(correctResult, myResult))
			{
				System.out.println("iterator中某个结果比较错误");
				return false;
			}
		}
		if(iter1.hasNext() || iter2.hasNext()){
			System.out.println("iterator比较错误,结果数量不对");
			if(iter1.hasNext()){
				System.out.println("正确答案有多余的result,他们的orderid为");
				while(iter1.hasNext())
					System.out.print(iter1.next().orderId()+"  ");
				System.out.println();
			}
			if(iter2.hasNext()){
				System.out.println("我的答案有多余的result,他们的orderid为");
				while(iter2.hasNext())
					System.out.print(iter2.next().orderId()+"  ");
				System.out.println();
			}
			return false;
		}
		return true;
	}
	
	private boolean compareKV(KeyValue kv1,KeyValue kv2)
	{
		if(kv1==null && kv2 == null){
			return true;
		}
		if(kv1 == null && kv2 != null){
			System.out.println("kv比较错误，正确答案为null。");
		}
		if(kv1 != null && kv2 == null){
			System.out.println("kv比较错误，我的答案为null，正确答案非null");
		}
		String key = kv1.key();
		String key2 = kv2.key();
		if(!key.equals(key2)){
			System.out.println("key比较错误");
			return false;
		}
		String value = kv1.valueAsString();
		String value2 = kv2.valueAsString();
		if(!value.equals(value2)){
			System.out.println("value比较错误，正确值："+value+" 我的值："+value2);
			return false;
		}
		return true;
	}
	
	private boolean compareResult(Result rightResult,Result myResult)
	{
		if(rightResult == null && myResult == null){
			return true;
		}
		if(rightResult == null && myResult != null){
			System.out.println("result比较错误，正确答案为null。我的答案非null");
			return false;
		}
		if(rightResult != null && myResult == null){
			System.out.println("result比较错误，我的答案为null，正确答案非null");
			return false;
		}
		if(rightResult.orderId() != myResult.orderId()){
			System.out.println("result中orderid比对错误，正确orderid:"
					+rightResult.orderId()+" 我的orderid："+myResult.orderId());
			return false;
		}
		KeyValue[] rightKVs = rightResult.getAll();
		KeyValue[] myKVs = myResult.getAll();
		return compareKVEqual(rightKVs, myKVs);
	}
	
	private boolean compareKVEqual(KeyValue[] rightKVs,KeyValue[] myKVs)
	{
		if(rightKVs == null && myKVs == null){
			return true;
		}
		if(rightKVs == null && myKVs != null){
			System.out.println("KV数组比对错误，正确答案为null。我的答案为："+myKVs);
			return false;
		}
		if(rightKVs != null && myKVs == null){
			System.out.println("KV数组比对错误，我的答案为null，正确答案为："+rightKVs);
			return false;
		}
		if(rightKVs.length != myKVs.length){
			System.out.println("KV数组比对错误,两个数组长度不相等。正确长度："
					+rightKVs.length+"我的数组长度："+myKVs.length);	
			System.out.println("正确的值：");
			for(int i=0;i<rightKVs.length;i++)
				System.out.println(rightKVs[i]);
			System.out.println("我的值：");
			for(int i=0;i<myKVs.length;i++)
				System.out.println(myKVs[i]);
			return false;
		}
		Map<String,KeyValue> rightKVMap = new HashMap<String,KeyValue>(rightKVs.length);
		for(KeyValue kv : rightKVs)
		{
			rightKVMap.put(kv.key(), kv);
		}
		Map<String,KeyValue> myKVMap = new HashMap<String,KeyValue>(rightKVs.length);
		for(KeyValue kv : myKVs)
		{
			myKVMap.put(kv.key(),kv);
		}
		if(!rightKVMap.keySet().containsAll(myKVMap.keySet()) 
				|| !myKVMap.keySet().containsAll(rightKVMap.keySet())){
			System.out.println("KV数组比对错误,两个数组的键集合不等价。正确键集合"
						+rightKVMap.keySet()+" 我的键集合："+myKVMap.keySet());
			return false;
		}
		for(KeyValue kv : rightKVs)
		{
			String key = kv.key();
			String rightValue = rightKVMap.get(key).valueAsString();
			String myValue = myKVMap.get(key).valueAsString();
			if(!rightValue.equals(myValue))
			{
				System.out.println("KV数组对于key"+key+"比对错误。正确value为："+rightValue+" "
						+ "我的value为："+myValue);
				return false;
			}
		}
		return true;
	}
	
	
	
}
