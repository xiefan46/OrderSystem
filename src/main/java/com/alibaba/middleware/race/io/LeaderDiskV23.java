package com.alibaba.middleware.race.io;

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
public class LeaderDiskV23 {
	private ArrayList<DiskV23> disks;
	
	private final OrderTreeV23 orderTree;
	
	private final InfoTreeV23 buyerTree;
	
	private final InfoTreeV23 goodTree;
	
	private final BuyerOrderTreeV23 buyerOrderTree;
	
	private final GoodOrderTreeV23 goodOrderTree;
	
	//TODO:可以把索引分散在不同的disk
	public LeaderDiskV23(ArrayList<DiskV23> disks)
	{
		this.disks = disks;
		DiskV23 disk0 = disks.get(0);
		DiskV23 disk1 = disks.get(1);
		DiskV23 disk2 = disks.get(2);
		this.orderTree = new OrderTreeV23(disk0.getStoreRootPath()+"order_tree_dir/");
		this.buyerTree = new InfoTreeV23(disk1.getStoreRootPath()+"buyer_tree_dir/", 
				"buyerid","buyer_tree_dir/");
		this.goodTree = new InfoTreeV23(disk2.getStoreRootPath()+"good_tree_dir/",
				"goodid","good_tree_dir/");
		this.buyerOrderTree = new BuyerOrderTreeV23(disk1.getStoreRootPath()+"buyer_order_tree_dir/");
		this.goodOrderTree = new GoodOrderTreeV23(disk2.getStoreRootPath()+"good_order_tree_dir/");
	}
	
	public void mergeAndConstuct(final ArrayList<DiskV23> disks) throws Exception
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

	public OrderTreeV23 getOrderTree() {
		return orderTree;
	}

	public InfoTreeV23 getBuyerTree() {
		return buyerTree;
	}

	public InfoTreeV23 getGoodTree() {
		return goodTree;
	}

	public BuyerOrderTreeV23 getBuyerOrderTree() {
		return buyerOrderTree;
	}

	public GoodOrderTreeV23 getGoodOrderTree() {
		return goodOrderTree;
	}

	
	
}
