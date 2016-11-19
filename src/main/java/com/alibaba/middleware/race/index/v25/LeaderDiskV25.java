package com.alibaba.middleware.race.index.v25;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.database.DatabaseV23;
import com.alibaba.middleware.race.tree.BuyerOrderTreeV23;
import com.alibaba.middleware.race.tree.GoodOrderTreeV23;
import com.alibaba.middleware.race.tree.InfoTreeV23;
import com.alibaba.middleware.race.tree.OrderTreeV23;



/*
 * 各种索引建在不同的磁盘上
 */
public class LeaderDiskV25 {
	private ArrayList<DiskV25> disks;
	
	private final OrderTreeV25 orderTree;
	
	private final InfoTreeV25 buyerTree;
	
	private final InfoTreeV25 goodTree;
	
	private final BuyerOrderTreeV25 buyerOrderTree;
	
	private final GoodOrderTreeV25 goodOrderTree;
	
	//TODO:可以把索引分散在不同的disk
	public LeaderDiskV25(ArrayList<DiskV25> disks)
	{
		this.disks = disks;
		DiskV25 disk0 = disks.get(0);
		DiskV25 disk1 = disks.get(1);
		DiskV25 disk2 = disks.get(2);
		this.orderTree = new OrderTreeV25(disk0.getStoreRootPath()+"order_tree_dir/");
		this.buyerTree = new InfoTreeV25("buyerid");
		this.goodTree = new InfoTreeV25("goodid");
		this.buyerOrderTree = new BuyerOrderTreeV25(disk1.getStoreRootPath()+"buyer_order_tree_dir/");
		this.goodOrderTree = new GoodOrderTreeV25(disk2.getStoreRootPath()+"good_order_tree_dir/");
	}
	
	public void mergeAndConstuct(final ArrayList<DiskV25> disks) throws Exception
	{
		
		final CountDownLatch latch = new CountDownLatch(5);
		DatabaseV23.executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					orderTree.construct(disks);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
		
		DatabaseV23.executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					buyerTree.construct(disks);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
		
		DatabaseV23.executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					goodTree.construct(disks);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
		
		DatabaseV23.executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					buyerOrderTree.construct(disks);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
		
		DatabaseV23.executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					goodOrderTree.construct(disks);
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					latch.countDown();
				}
			}
		});
		
		latch.await();
		
	}

	public OrderTreeV25 getOrderTree() {
		return orderTree;
	}

	public InfoTreeV25 getBuyerTree() {
		return buyerTree;
	}

	public InfoTreeV25 getGoodTree() {
		return goodTree;
	}

	public BuyerOrderTreeV25 getBuyerOrderTree() {
		return buyerOrderTree;
	}

	public GoodOrderTreeV25 getGoodOrderTree() {
		return goodOrderTree;
	}

	
	
}
