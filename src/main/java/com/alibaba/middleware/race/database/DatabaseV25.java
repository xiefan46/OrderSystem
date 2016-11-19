package com.alibaba.middleware.race.database;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;




import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.config.RaceConfig.ExistenceType;
import com.alibaba.middleware.race.index.model.LightRow;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.v25.DiskV25;
import com.alibaba.middleware.race.index.v25.LeaderDiskV25;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.LeaderDiskV23;
import com.alibaba.middleware.race.model.KV;
import com.alibaba.middleware.race.model.ResultImpl;
import com.alibaba.middleware.race.model.Row;


/*
 *在V23版本基础上，good和buyer改为直接索引
 */

public class DatabaseV25 implements OrderSystem{
	
	//v10
	private ArrayList<DiskV25> disks = new ArrayList<DiskV25>();
	
	private LeaderDiskV25 leaderDisk;

	public static ExecutorService executorService = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors()*2);
	
	private Object lock = new Object();
	
	private final AtomicBoolean ok = new AtomicBoolean(false);
	
	

	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,InterruptedException 
	{
		long constructStart = System.currentTimeMillis();
		try {
			/*
			 * 整理文件到对应的磁盘中
			 */
			System.out.println("开始整理文件到对应磁盘");
			int diskId = 0;
			HashMap<String,DiskV25> diskMap = new HashMap<String,DiskV25>();
			for(String folder : storeFolders)
			{
				String[] strs = folder.split("/");
				String diskName = strs[1];
				DiskV25 disk = new DiskV25(diskId, folder, diskName, executorService);
				diskMap.put(diskName, disk);
				disks.add(disk);
				diskId++;
			}
			for(String str : orderFiles)
			{
				String[] strs = str.split("/");
				DiskV25 disk = diskMap.get(strs[1]);
				if(disk == null){
					throw new Exception("有订单文件不在任何已知磁盘上");
				}
				disk.putOrderFile(str);
			}
			for(String str : buyerFiles)
			{
				String[] strs = str.split("/");
				DiskV25 disk = diskMap.get(strs[1]);
				if(disk == null){
					throw new Exception("有买家文件不在任何已知磁盘上");
				}
				disk.putBuyerFile(str);
			}
			for(String str : goodFiles)
			{
				String[] strs = str.split("/");
				DiskV25 disk = diskMap.get(strs[1]);
				if(disk == null){
					throw new Exception("有商品文件不在任何已知磁盘上");
				}
				disk.putGoodFile(str);
			}
			
			for(int i=0;i<disks.size();i++)
			{
				DiskV25 disk = disks.get(i);
				System.out.println("-------------disk info---------------");
				System.out.println("disk id:"+disk.getDiskId());
				System.out.println("order files:"+disk.getOrderFiles());
				System.out.println("order files:"+disk.getBuyerFiles());
				System.out.println("order files:"+disk.getGoodFiles());
				long orderSize = 0;
				long buyerSize = 0;
				long goodSize = 0;
				//统计各个磁盘上各种文件的总大小
				for(String str : disk.getOrderFiles())
				{
					orderSize += new File(str).length();
				}
				for(String str : disk.getBuyerFiles())
				{
					buyerSize += new File(str).length();
				}
				for(String str : disk.getGoodFiles())
				{
					goodSize += new File(str).length();
				}
				System.out.println("orderSize:"+orderSize/(1024*1024)+"MB");
				System.out.println("buyerSize:"+buyerSize/(1024*1024)+"MB");
				System.out.println("goodSize:"+goodSize/(1024*1024)+"MB");
				System.out.println("----------------------------");
			}
			
			/*
			 * 扫描订单文件、买家文件、商品文件，建立排序好的二级索引小文件
			 */
			System.out.println("开始分磁盘扫描文件，建立排序好的二级索引小文件");
			long startTime = System.currentTimeMillis();
			final CountDownLatch orderLatch = new CountDownLatch(disks.size());
			for(DiskV25 disk : disks) 
			{
				disk.scanOrderFiles(orderLatch);
			}
			orderLatch.await();
			System.out.println("scan order time:"+(System.currentTimeMillis() - startTime));
			
			startTime = System.currentTimeMillis();
			final CountDownLatch buyerLatch = new CountDownLatch(disks.size());
			for(DiskV25 disk : disks) 
			{
				disk.scanBuyerFiles(buyerLatch);
			}
			buyerLatch.await();
			System.out.println("scan buyer time:"+(System.currentTimeMillis() - startTime));
			
			startTime = System.currentTimeMillis();
			final CountDownLatch goodLatch = new CountDownLatch(disks.size());
			for(DiskV25 disk : disks) 
			{

				disk.scanGoodFiles(goodLatch);
			}
			goodLatch.await();
			System.out.println("scan good time:"+(System.currentTimeMillis() - startTime));
			
			/*final CountDownLatch orderLatch = new CountDownLatch(disks.size());
			final CountDownLatch buyerLatch = new CountDownLatch(disks.size());
			final CountDownLatch goodLatch = new CountDownLatch(disks.size());
			for(Disk disk : disks)
			{
				disk.scanOrderFiles(orderLatch);
				disk.scanBuyerFiles(buyerLatch);
				disk.scanGoodFiles(goodLatch);
			}
			orderLatch.await();
			buyerLatch.await();
			goodLatch.await();*/
			/*
			 * 启动线程归并文件到leader disk
			 */	
			this.leaderDisk = new LeaderDiskV25(disks);
			executorService.execute(new MergeTask(this.leaderDisk, disks, lock));
			long canSleepTime = 1000*3500 - (System.currentTimeMillis() - constructStart);
			//long canSleepTime = 0;
			if(canSleepTime > 0) 
				Thread.currentThread().sleep(canSleepTime);
			//Thread.currentThread().sleep(10000);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
	}
	
	class MergeTask implements Runnable{
		
		LeaderDiskV25 leader;
		
		ArrayList<DiskV25> disks;
		
		Object lock;
		
		public MergeTask(LeaderDiskV25 leader,ArrayList<DiskV25> disks,Object lock){
			this.leader = leader;
			this.disks = disks;
			this.lock = lock;
		}
		
		@Override
		public void run() {
			try{
				//System.gc();
				//Thread.currentThread().sleep(10000);
				//long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
				//long startMemInMB = startMem / (1024*1024);
				//.out.println("构建索引前内存占用：" + startMemInMB +" MB");
				long startTime = System.currentTimeMillis();
				System.out.println("开始归并小文件到leader disk");
				leader.mergeAndConstuct(disks);
				for(DiskV25 disk : disks)
					disk.start();
				System.out.println("build first index time:"+(System.currentTimeMillis() - startTime));
				//System.gc();
				//Thread.currentThread().sleep(10000);
				//long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
				//long endMemInMB = endMem / (1024*1024);
				//System.out.println("构建索引后内存占用:"+endMemInMB+" MB");
				
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				ok.getAndSet(true);
				
				synchronized (lock) {
					lock.notifyAll();
				}
			}
		}
		
	}

	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		try {
			if(!ok.get())
			{
				synchronized (lock) {
					lock.wait();
				}
			}
			
			LightRow order = queryNecessaryOrderByKey(orderId, keys);
			if(order == null) return null;
			return ResultImpl.createResultRow(order, createQueryKeys(keys));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private LightRow queryNecessaryOrderByKey(long orderid, Collection<String> keys) throws Exception
	{
		if(keys == null){ //需要全查找的情况，无法优化
			LightRow row = queryOrderJoinBuyerJoinGood(orderid);
			if(row == null) return null;
			return row;
		}
		if(keys.isEmpty() || orderIdOnly(keys)) //只需要判断这条order是否存在
		{
			boolean isExist = testOrderIdExist(orderid);
			if(isExist){
				LightRow row = new LightRow();
				row.put("orderid", orderid+"");
				return row;
			}else{
				return null;
			}
		}
		LightRow order = leaderDisk.getOrderTree().queryOnlyOrderById(orderid);
		if(order == null) return null;
		Set<String> allKey = new HashSet<String>();
		allKey.addAll(order.keySet());
		if(allKey.containsAll(keys)){  //order中已经拥有所有要查的字段
			return order;
		}
		String goodid = order.get("goodid");
		LightRow good = leaderDisk.getGoodTree().queryStringIdRecordById(goodid);
		//System.out.println("good:"+good);
		if(good == null){
			throw new Exception("order中的good信息不存在");
		}
		allKey.addAll(good.keySet());
		order.putAll(good);
		if(allKey.containsAll(keys)){  //order中已经拥有所有要查的字段
			return order;
		}
		String buyerid = order.get("buyerid");
		LightRow buyer = leaderDisk.getBuyerTree().queryStringIdRecordById(buyerid);
		if(buyer == null){
			throw new Exception("order中的good信息不存在");
		}
		order.putAll(buyer);
		return order;
	}
	
	private boolean orderIdOnly(Collection<String> keys)
	{
		if(keys == null) return false;
		if(keys.size() == 1 && keys.contains("orderid")) return true;
		return false;
	}
	
	private boolean testOrderIdExist(long orderid) throws Exception
	{
		return leaderDisk.getOrderTree().testOrderIdExist(orderid);
	}
	
	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) 
	{
		try {
			if(!ok.get())
			{
				synchronized (lock) {
					lock.wait();
				}
			}
			
			List<Result> resultList = new ArrayList<Result>();
			if(buyerid == null) return resultList.iterator();
			ArrayList<OrderPosInfo> orderPos = leaderDisk.getBuyerOrderTree().getOrderPosByBuyerid(buyerid, 
					startTime, endTime);
			//System.out.println("找到buyerid对应的orderid有："+orderIds);
			if(orderPos == null || orderPos.size() == 0) {
				return resultList.iterator();
			}
			//时间从大到小排
			Collections.reverse(orderPos);
			ArrayList<LightRow> orders = queryOrdersAsync(orderPos);
			for(LightRow order : orders)
			{
				resultList.add(ResultImpl.createResultRow(order, null));
			}
			return resultList.iterator();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * 这个接口中salerid暂时没用
	 */
	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		try {
			if(!ok.get())
			{
				synchronized (lock) {
					lock.wait();
				}
			}
			//System.out.println("goodid:"+goodid);
			List<Result> resultList = new ArrayList<Result>();
			if(goodid == null) return resultList.iterator();
			
			ArrayList<OrderPosInfo> orderPos = leaderDisk.getGoodOrderTree().getOrderPosByGoodid(goodid);
			//TODO：如果buyerid找不到或者没有对应时间范围的订单，是返回一个空的iterator还是返回null
			if(orderPos == null || orderPos.size() ==0) {
				return resultList.iterator();
			}
			ArrayList<LightRow> orders = queryOrdersAsync(orderPos);
			Collections.sort(orders, new Comparator<LightRow>() {

				@Override
				public int compare(LightRow o1, LightRow o2) {
					long id1 = Long.parseLong(o1.get("orderid"));
					long id2 = Long.parseLong(o2.get("orderid"));
					return Long.compare(id1, id2);
				}
			});
			for(LightRow order : orders)
			{
				resultList.add(ResultImpl.createResultRow(order, createQueryKeys(keys)));
			}
			return resultList.iterator();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private ArrayList<LightRow> queryOrdersAsync(ArrayList<OrderPosInfo> posInfos) throws Exception
	{
		ArrayList<FutureTask<LightRow>> tasks = new ArrayList<FutureTask<LightRow>>();
		ArrayList<LightRow> orders = new ArrayList<LightRow>();
		for(final OrderPosInfo pos : posInfos)
		{
			tasks.add(new FutureTask<LightRow>(new Callable<LightRow>() {
				@Override
				public LightRow call() throws Exception {
					return queryOrderJoinBuyerJoinGoodByPos(pos);
				}
			}));
		}
		for(FutureTask<LightRow> task : tasks)
			executorService.submit(task);
		for(FutureTask<LightRow> task : tasks)
		{
			orders.add(task.get());
		}
		return orders;
	}

	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		try{
			if(!ok.get())
			{
				synchronized (lock) {
					lock.wait();
				}
			}
			
			if(goodid == null || key == null) return null;
			HashSet<String> queryingKeys = new HashSet<String>();
		    queryingKeys.add(key);
			Iterator<Result> iter = queryOrdersBySaler("123", goodid,queryingKeys);
			List<Result> allData = new ArrayList<Result>();
			while(iter.hasNext())
				allData.add(iter.next());
			KeyValue kv = sumResultByKey(allData, key);
			return kv;
			
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	private LightRow queryOrderJoinBuyerJoinGood(long orderid) throws Exception
	{
		LightRow order = leaderDisk.getOrderTree().queryOnlyOrderById(orderid);
		if(order == null) return null;
		LightRow buyer = queryBuyerByOrder(order);
		if(buyer == null){
			throw new Exception("订单信息中的buyer信息无效");
		}
		LightRow good = queryGoodByOrder(order);
		if(good == null){
			throw new Exception("订单信息中的good信息无效");
		}
		order.putAll(buyer);
		order.putAll(good);
		return order;
	}
	
	private LightRow queryOrderJoinBuyerJoinGoodByPos(OrderPosInfo posInfo) throws Exception
	{
		LightRow order = this.leaderDisk.getOrderTree().queryOnlyOrderByOrderPos(posInfo);
		if(order == null) return null;
		LightRow buyer = queryBuyerByOrder(order);
		if(buyer == null){
			throw new Exception("订单信息中的buyer信息无效");
		}
		LightRow good = queryGoodByOrder(order);
		if(good == null){
			throw new Exception("订单信息中的good信息无效");
		}
		order.putAll(buyer);
		order.putAll(good);
		return order;
	}
	
	
	
	private LightRow queryBuyerByOrder(LightRow order) throws Exception
	{
		String buyerId = order.get("buyerid");
		LightRow buyer = leaderDisk.getBuyerTree().queryStringIdRecordById(buyerId);
		return buyer;
	}
	
	private LightRow queryGoodByOrder(LightRow order) throws Exception
	{
		String goodId = order.get("goodid");
		LightRow good = leaderDisk.getGoodTree().queryStringIdRecordById(goodId);
		return good;
	}

	private Row createRowFromLightRow(LightRow lrow)
	{
		Row row = new Row();
		for(Entry<String,String> entry : lrow.entrySet())
		{
			row.putKV(entry.getKey(), entry.getValue());
		}
		return row;
	}
	
	private HashSet<String> createQueryKeys(Collection<String> keys)
	{
		    if (keys == null) {
		      return null;
		    }
		    return new HashSet<String>(keys);
	}
	
	private KeyValue sumResultByKey(Collection<Result> allData,String key)
	{
		try {
	        boolean hasValidData = false;
	        long sum = 0;
	        for (Result r : allData) {
	          KeyValue kv = r.get(key);
	          if (kv != null) {
	            sum += kv.valueAsLong();
	            hasValidData = true;
	          }
	        }
	        if (hasValidData) {
	          return new KV(key, Long.toString(sum));
	        }
	      } catch (TypeException e) {
	      }

	      // accumulate as double
	      try {
	        boolean hasValidData = false;
	        double sum = 0;
	        for (Result r : allData) {
	          KeyValue kv = r.get(key);
	          if (kv != null) {
	            sum += kv.valueAsDouble();
	            hasValidData = true;
	          }
	        }
	        if (hasValidData) {
	          return new KV(key, Double.toString(sum));
	        }
	      } catch (TypeException e) {
	      }
		return null;
	}
	
	
}


