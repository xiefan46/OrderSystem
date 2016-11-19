package com.alibaba.middleware.race;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.database.DatabaseV23;
import com.alibaba.middleware.race.database.DatabaseV25;
import com.alibaba.middleware.race.database.RightOrderSystem;
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.model.KV;
import com.alibaba.middleware.race.model.ResultImpl;
import com.alibaba.middleware.race.model.Row;
import com.alibaba.middleware.race.sort.ExternalSorting;

public class OrderSystemImpl implements OrderSystem {
	
	private OrderSystem db = new DatabaseV25();
	
	public OrderSystemImpl(){
		
	}
	
	public static Logger log = Logger.getLogger("lavasoft");
    

	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		System.out.println("文件路径为：");
		System.out.println("order:");
		for(String str : orderFiles)
			System.out.println(str);
		System.out.println("storeFolder");
		for(String str : storeFolders)
			System.out.println(str);
		log.setLevel(Level.INFO);
		//long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		//long startMemInMB = startMem / (1024*1024);
		//System.out.println("生成前内存占用：" + startMemInMB +" MB");
		long startTime = System.currentTimeMillis();
		db.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
		long passTime = System.currentTimeMillis() - startTime;
		//System.out.println("数据库construct总体消耗时间："+ passTime / 1000 +"秒");
		System.out.println("数据库construct总体消耗时间："+ passTime  +"ms");
		//System.gc();
		//Thread.currentThread().sleep(10000);
		
	}
	//测时间
	/*private AtomicInteger c1 = new AtomicInteger(1);
	private AtomicInteger c2 = new AtomicInteger(1);
	private AtomicInteger c3 = new AtomicInteger(1);
	private AtomicInteger c4 = new AtomicInteger(1);
	private AtomicLong t1 = new AtomicLong(0);
	private AtomicLong t2 = new AtomicLong(0);
	private AtomicLong t3 = new AtomicLong(0);
	private AtomicLong t4 = new AtomicLong(0);*/
	
	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		//long startTime = System.currentTimeMillis();
		//log.info("query order. keys:"+keys);
		Result result = db.queryOrder(orderId, keys);
		/*t1.addAndGet(System.currentTimeMillis() - startTime);
		if(c1.incrementAndGet() % 100 == 0){
			System.out.println("第一个接口被查询次数:"+c1.get());
			System.out.println("第一个接口平均耗时:"+t1.get()/c1.get());
		}*/
		//log.info("query order. orderid:"+orderId+" result"+result);
		return result;
	}

	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {
		//long begin = System.currentTimeMillis();
		//System.out.println("query orders by buyer. startTime:"+startTime+" endTime:"+endTime);
		Iterator<Result> result = db.queryOrdersByBuyer(startTime, endTime, buyerid);
		/*t2.addAndGet(System.currentTimeMillis() - begin);
		if(c2.incrementAndGet() % 100 == 0){
			System.out.println("第二个接口被查询次数:"+c2.get());
			System.out.println("第二个接口平均耗时:"+t2.get()/c2.get());
		}*/
		return result;
	}

	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		//long begin = System.currentTimeMillis();
		//System.out.println("queryOrdersBySaler. keys:"+keys);
		Iterator<Result> result = db.queryOrdersBySaler(salerid, goodid, keys);
		/*t3.addAndGet(System.currentTimeMillis() - begin);
		if(c3.incrementAndGet() % 100 == 0){
			System.out.println("第三个接口被查询次数:"+c3.get());
			System.out.println("第三个接口平均耗时:"+t3.get()/c3.get());
		}*/
		//if(result != null) System.out.println("sumOrdersByGood. goodid:"+goodid+" result:"+result);
		//else System.out.println("sumOrdersByGood. goodid:"+goodid+" result:null");
		return result;
	}

	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		//long begin = System.currentTimeMillis();
		//System.out.println("sumOrdersByGood. key:"+key);
		KeyValue result = db.sumOrdersByGood(goodid, key);
		//if(result != null) System.out.println("sumOrdersByGood. goodid:"+goodid+" result:"+result);
		//else System.out.println("sumOrdersByGood. goodid:"+goodid+" result:null");
		/*t4.addAndGet(System.currentTimeMillis() - begin);
		if(c4.incrementAndGet() % 100 == 0){
			System.out.println("第四个接口被查询次数:"+c4.get());
			System.out.println("第四个接口平均耗时:"+t4.get()/c4.get());
		}*/
		return result;
	}
	
	
	

}
