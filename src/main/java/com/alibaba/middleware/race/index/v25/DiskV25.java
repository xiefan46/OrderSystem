package com.alibaba.middleware.race.index.v25;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.LightRow;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.OrderSecondIndexV13;
import com.alibaba.middleware.race.index.model.StringSecondIndexV13;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.parser.RowParserMulti;
import com.alibaba.middleware.race.manager.BuyerOrderManagerV23;
import com.alibaba.middleware.race.manager.GoodOrderManagerV23;
import com.alibaba.middleware.race.manager.InfoManagerV13;
import com.alibaba.middleware.race.manager.OrderManagerV13;
import com.alibaba.middleware.race.model.Row;

public class DiskV25 {
	
	private int diskId;
	
	private String storeRootPath;
	
	private String name;
	
	private ArrayList<String> orderFiles = new ArrayList<String>();
	
	private ArrayList<String> buyerFiles = new ArrayList<String>();
	
	private ArrayList<String> goodFiles = new ArrayList<String>();
	
	
	private final OrderManagerV13 orderManager;
	
	private final InfoManagerV25 buyerManager;
	
	private final InfoManagerV25 goodManager;
	
	private final BuyerOrderManagerV23 buyerOrderManager;
	
	private final GoodOrderManagerV23 goodOrderManager;
	
	private ExecutorService executor;
	
	//private GenericObjectPool<RowParserMulti> orderParserPool;
	
	//private GenericObjectPool<RowParserMulti> buyerParserPool;
	
	//private GenericObjectPool<RowParserMulti> goodParserPool;
	
	private int threadMax = 3;
	
	private RowParserMulti[] orderParsers = new RowParserMulti[threadMax];
	
	private RowParserMulti[] goodParsers = new RowParserMulti[threadMax];
	
	private RowParserMulti[] buyerParsers = new RowParserMulti[threadMax];
	
	private int[] order_lock;
	
	private int[] good_lock;
	
	private int[] buyer_lock;
	
	
	
	private IOThreadV3 ioThread = new IOThreadV3();
	
	public DiskV25(int diskId,String storeRootPath,String name,ExecutorService executor) throws Exception
	{
		this.diskId = diskId;
		this.storeRootPath = storeRootPath;
		this.name = name;
		this.executor = executor;
		this.orderManager = new OrderManagerV13(this.storeRootPath+"order_manager_dir/");
		this.buyerManager = new InfoManagerV25(this.storeRootPath+"buyer_manager_dir/");
		this.goodManager = new InfoManagerV25(this.storeRootPath+"good_manager_dir/");
		this.buyerOrderManager = new BuyerOrderManagerV23(this.storeRootPath+"buyer_order_manager_dir/");
		this.goodOrderManager = new GoodOrderManagerV23(this.storeRootPath+"good_order_manager_dir/");
	}
	
	public void putOrderFile(String f){
		this.orderFiles.add(f);
	}
	
	public void putBuyerFile(String f){
		this.buyerFiles.add(f);
	}
	
	public void putGoodFile(String f){
		this.goodFiles.add(f);
	}
	
	//public static AtomicLong totalCount = new AtomicLong(0);
	
	public void scanOrderFiles(final CountDownLatch latch) throws Exception
	{
		this.executor.execute(new Runnable() {	
			@Override
			public void run() {
				try{
					RowParserMulti parser = new RowParserMulti(orderFiles);
					long pos = parser.getPos();
					LightRow row = parser.readRow();
					while(row != null)
					{
						//System.out.println("读到row:"+row);
						long orderid = Long.parseLong(row.get("orderid"));
						String buyerid = row.get("buyerid");
						long createtime = Long.parseLong(row.get("createtime"));
						String goodid = row.get("goodid");
						orderManager.handle(orderid,pos);
						goodOrderManager.handleOrder(goodid,pos);
						buyerOrderManager.handle(buyerid,pos,createtime);
						pos = parser.getPos();
						row = parser.readRow();
					}	
					orderManager.finish();
					buyerOrderManager.finish();
					goodOrderManager.finish();
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
	}
	
	
	public void scanBuyerFiles(final CountDownLatch latch)
	{
		this.executor.execute(new Runnable() {
			
			@Override
			public void run() {
				try{
					RowParserMulti parser = new RowParserMulti(buyerFiles);
					long pos = parser.getPos();
					LightRow row = parser.readRow();
					while(row != null)
					{
						String buyerid = row.get("buyerid");
						//System.out.println("拿到buyerid:"+buyerid);
						buyerManager.handle(buyerid,pos);
						pos = parser.getPos();
						row = parser.readRow();
					}	
					buyerManager.finish();
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
	}
	
	public void scanGoodFiles(final CountDownLatch latch)
	{
		this.executor.execute(new Runnable() {
			
			@Override
			public void run() {
				try{
					RowParserMulti parser = new RowParserMulti(goodFiles);
					long pos = parser.getPos();
					LightRow row = parser.readRow();
					while(row != null)
					{
						String goodid = row.get("goodid");
						goodManager.handle(goodid,pos);
						pos = parser.getPos();
						row = parser.readRow();
					}	
					goodManager.finish();
				}catch(Exception e){
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
	}
	
	public void start() throws Exception
	{
		//this.orderParserPool = RowParserPoolFactory.createRowParserPool(orderFiles);
		//this.buyerParserPool = RowParserPoolFactory.createRowParserPool(buyerFiles);
		//this.goodParserPool = RowParserPoolFactory.createRowParserPool(goodFiles);
		initLock();
		for(int i=0;i<threadMax;i++)
		{
			orderParsers[i] = new RowParserMulti(orderFiles);
			buyerParsers[i] = new RowParserMulti(buyerFiles);
			goodParsers[i] = new RowParserMulti(goodFiles);
		}
		executor.execute(this.ioThread);
	}
	
	public LightRow queryOrder(OrderSecondIndexV13 secondIndex)
	{
		return queryOrderByPos(secondIndex.getOffset());
	}
	
	public LightRow queryOrderByPos(long orderpos)
	{
		int lockid = -1;
		try {
			lockid = getlock(order_lock);
			orderParsers[lockid].seek(orderpos);
			LightRow row = orderParsers[lockid].readRow();
			/*if(row == null)System.out.println("parse找到的row为null");
			else System.out.println(row);*/
			return row;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(lockid != -1)
				releaseLock(lockid, order_lock);
		}
		return null;
	}
	
	public LightRow queryBuyer(long offset)
	{
		int lockid = -1;
		try {
			lockid = getlock(buyer_lock);
			buyerParsers[lockid].seek(offset);
			LightRow row = buyerParsers[lockid].readRow();
			return row;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(lockid != -1)
				releaseLock(lockid, buyer_lock);
		}
		return null;
	}
	
	public LightRow queryGood(long offset)
	{
		int lockid = -1;
		try {
			lockid = getlock(good_lock);
			goodParsers[lockid].seek(offset);
			LightRow row = goodParsers[lockid].readRow();
			return row;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(lockid != -1)
				releaseLock(lockid, good_lock);
		}
		return null;
	}
	

	public int getDiskId() {
		return diskId;
	}

	public void setDiskId(int diskId) {
		this.diskId = diskId;
	}

	public String getStoreRootPath() {
		return storeRootPath;
	}

	public void setStoreRootPath(String storeRootPath) {
		this.storeRootPath = storeRootPath;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public OrderManagerV13 getOrderManager() {
		return orderManager;
	}

	public InfoManagerV25 getBuyerManager() {
		return buyerManager;
	}

	public InfoManagerV25 getGoodManager() {
		return goodManager;
	}

	public BuyerOrderManagerV23 getBuyerOrderManager() {
		return buyerOrderManager;
	}

	public GoodOrderManagerV23 getGoodOrderManager() {
		return goodOrderManager;
	}

	public IOThreadV3 getIoThread() {
		return ioThread;
	}

	public void setIoThread(IOThreadV3 ioThread) {
		this.ioThread = ioThread;
	}

	public ArrayList<String> getOrderFiles() {
		return orderFiles;
	}

	public void setOrderFiles(ArrayList<String> orderFiles) {
		this.orderFiles = orderFiles;
	}

	public ArrayList<String> getBuyerFiles() {
		return buyerFiles;
	}

	public void setBuyerFiles(ArrayList<String> buyerFiles) {
		this.buyerFiles = buyerFiles;
	}

	public ArrayList<String> getGoodFiles() {
		return goodFiles;
	}

	public void setGoodFiles(ArrayList<String> goodFiles) {
		this.goodFiles = goodFiles;
	}
	
	void initLock() {
		order_lock = new int[threadMax];
		good_lock = new int[threadMax];
		buyer_lock = new int[threadMax];
	}
	
	int getlock(int[] lock) throws InterruptedException {
		while(true) {
			synchronized (lock) {
				for (int i = 0; i < threadMax; i++) {
					if(lock[i] == 0) {
						lock[i] = 1;
						return i;
					}
				}
			}
			Thread.sleep(100);
		}
	}
	
	void releaseLock(int k,int[] lock) {
		synchronized (lock) {
			lock[k] = 0;
		}
	}
	
	
	
}
