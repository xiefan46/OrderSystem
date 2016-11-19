package com.alibaba.middleware.race.index.v25;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.BuyerOrderEntryV23;
import com.alibaba.middleware.race.index.model.BuyerOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.BuyerOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.OrderposTime;
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.TaskV3;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.manager.BuyerOrderManagerV23;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BuyerOrderTreeV25 {
	
	private String storeRootPath;
	
	private ArrayList<DiskV25> disks;
	
	private static final String secondIndexFileName = "secondIndexFile";
	
	//private RandomAccessFile secondIndexRaf;
	private TreeMap<String, StoreInfo> firstIndex = new TreeMap<String, StoreInfo>();
	
	private static final String dirName = "buyer_order_tree_dir/";
	
	private RandomAccessFile[] secondIndexRafs;
	
	private String[] rafNames;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	public BuyerOrderTreeV25(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void construct(ArrayList<DiskV25> disks) throws Exception
	{
		this.disks = disks;
		mergeSortedSecondIndexFiles();
		secondIndexRafs = new RandomAccessFile[disks.size()];
		rafNames = new String[disks.size()];
		for(int i=0;i<disks.size();i++)
		{
			DiskV25 disk = disks.get(i);
			String name = disk.getStoreRootPath()+dirName;
			if(name.equals(this.storeRootPath))
				rafNames[i] = name + secondIndexFileName;
			else{
				String fileName = RaceUtil.copyFileToFolder(storeRootPath+secondIndexFileName,
						name);
				System.out.println("copy file:"+fileName);
				rafNames[i] = fileName;
			}
		}
		for(int i=0;i<rafNames.length;i++)
		{
			System.out.println(rafNames[i]);
			secondIndexRafs[i] = new RandomAccessFile(rafNames[i], "r");
			System.out.println("rsf size:"+secondIndexRafs[i].length());
		}
		
	}
	
	/*public ArrayList<Long> getOrderIdsByBuyerid(String buyerid,long startTime,long endTime) throws Exception
	{
		int rafId = count.incrementAndGet() % disks.size();
		//System.out.println("本次查询的buyerid为："+buyerid);
		Entry<String,StoreInfo> entry = this.firstIndex.floorEntry(buyerid);
		if(entry == null) return null;
		StoreInfo storeInfo = entry.getValue();
		TaskV3 task1 = new TaskV3(storeInfo.getOffset(),storeInfo.getLength(),secondIndexRafs[rafId]);
		this.disks.get(rafId).getIoThread().submitTask(task1);
		byte[] data = task1.getIOResult();
		Kryo kryo = KryoUtil.borrow();
		Input input = new Input(data);
		BuyerOrderFirstIndex block = kryo.readObject(input, BuyerOrderFirstIndex.class);
		//System.out.println("block id 为："+block.getId());
		BuyerOrderSecondIndex map = block.find(buyerid);
		if(map == null) return null;
		TreeSet<OrderIdTime> set = map.getTreeSet();
		if(set == null || set.isEmpty()) {
			System.out.println("查找到的set为空");
			return null;
		}
		SortedSet<OrderIdTime> subset = set.subSet(new OrderIdTime(Long.MIN_VALUE,startTime), 
				new OrderIdTime(Long.MAX_VALUE, endTime-1));
		if(subset == null || subset.size() == 0) 
			return null;
		ArrayList<Long> orderIds = new ArrayList<Long>(subset.size());
		for(OrderIdTime oit : subset)
			orderIds.add(oit.getOrderId());
		return orderIds;
	}*/
	public ArrayList<OrderPosInfo> getOrderPosByBuyerid(String buyerid,long startTime,long endTime) throws Exception
	{
		int rafId = count.incrementAndGet() % disks.size();
		//System.out.println("本次查询的buyerid为："+buyerid);
		Entry<String,StoreInfo> entry = this.firstIndex.floorEntry(buyerid);
		if(entry == null) return null;
		StoreInfo storeInfo = entry.getValue();
		TaskV3 task1 = new TaskV3(storeInfo.getOffset(),storeInfo.getLength(),secondIndexRafs[rafId]);
		this.disks.get(rafId).getIoThread().submitTask(task1);
		byte[] data = task1.getIOResult();
		Kryo kryo = KryoUtil.borrow();
		Input input = new Input(data);
		BuyerOrderFirstIndexV23 block = kryo.readObject(input, BuyerOrderFirstIndexV23.class);
		//System.out.println("block id 为："+block.getId());
		BuyerOrderSecondIndexV23 map = block.find(buyerid);
		if(map == null) return null;
		TreeSet<OrderposTime> set = map.getTreeSet();
		OrderposTime start = new OrderposTime(new OrderPosInfo(Long.MIN_VALUE, Integer.MIN_VALUE),startTime);
		OrderposTime end = new OrderposTime(new OrderPosInfo(Long.MAX_VALUE, Integer.MAX_VALUE),endTime-1);
		SortedSet<OrderposTime> subset = set.subSet(start,end);
		ArrayList<OrderPosInfo> orderPos = new ArrayList<OrderPosInfo>(subset.size());
		for(OrderposTime oit : subset)
			orderPos.add(oit.getPosInfo());
		return orderPos;
	}
	
	private void mergeSortedSecondIndexFiles() throws Exception
	{
		File outputFile = new File(storeRootPath + secondIndexFileName);
		if(!outputFile.exists())
			outputFile.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		List<BinaryFileBuffer> buffers = new ArrayList<BinaryFileBuffer>();
		for(int i=0;i<disks.size();i++)
		{
			
			DiskV25 disk = disks.get(i);
			BuyerOrderManagerV23 manager = disk.getBuyerOrderManager();
			for(File f : manager.getSortedBuyerOrderSmallFile())
			{
				 Input input = new Input(new FileInputStream(f),4096);
		         Kryo kryo = KryoUtil.borrow();
		         BinaryFileBuffer bfb = new BinaryFileBuffer(kryo,input,disk.getDiskId());
		         buffers.add(bfb);
			}
		}
		mergeSortedFiles(bos, buffers);
	}
	
	private void mergeSortedFiles(BufferedOutputStream bos,List<BinaryFileBuffer> buffers) throws Exception 
	{       
            PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
                    11, new Comparator<BinaryFileBuffer>() {
                            @Override
                            public int compare(BinaryFileBuffer i,
                                    BinaryFileBuffer j) {
                                    return i.peek().compareTo(j.peek());
                            }
                    });
            
            for (BinaryFileBuffer bfb : buffers){
            	 if (!bfb.empty())  pq.add(bfb);
            }
            //System.out.println("准备开始归并");
            BuyerOrderSecondIndexV23 map = new BuyerOrderSecondIndexV23();
            BuyerOrderFirstIndexV23 buyerBlock = new BuyerOrderFirstIndexV23();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Kryo kryo = KryoUtil.borrow();
            Output output = new Output(baos);
            long curOffset = 0;
            //long count = 0;
            try {                        
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	BuyerOrderEntryV23 minEntry = bfb.pop();
                	/*count++;
                	if(count % 1000000 == 0){
                		System.out.println("buyer order归并到第："+count+"条");
                	}*/
                	if(map.getBuyerid() == null || map.getBuyerid().equals(minEntry.getBuyerid())){
                		map.addEntry(minEntry,bfb.getDiskId());
                	}else {
                		buyerBlock.putBlock(map);
                		map = new BuyerOrderSecondIndexV23();
                		map.addEntry(minEntry,bfb.getDiskId());
                	}
                	//写入二级索引并同时在上面建立一级索引
                	if(buyerBlock.isFull())
                	{
                		kryo.writeObject(output, buyerBlock);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		this.firstIndex.put(buyerBlock.getId(), new StoreInfo(curOffset, length));
                		curOffset += length;
                		bos.write(data);
                		baos.reset();
                		buyerBlock = new BuyerOrderFirstIndexV23();
                	}
                	
                    if (bfb.empty()) {
                      //bfb.close();
                      pq.remove(bfb);
                     } else {
                      pq.add(bfb); // add it back
                     }
                 }
            } finally {
            	    if(!map.isEmpty()){
            	    	buyerBlock.putBlock(map);
            	    }
            		if(!buyerBlock.isEmpty()){
            			kryo.writeObject(output, buyerBlock);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		this.firstIndex.put(buyerBlock.getId(), new StoreInfo(curOffset, length));
                		curOffset += length;
                		bos.write(data);
            		}
            		output.close();
            		bos.flush();
            		bos.close();
                    KryoUtil.release(kryo);
                    for (BinaryFileBuffer bfb : pq)
                            bfb.close();
            }
           // System.out.println("归并完成");
            
    }
	
	
	final class BinaryFileBuffer {
	    public BinaryFileBuffer(Kryo kryo,Input input,int diskId) throws IOException {
	            this.input = input;
	            this.kryo = kryo;
	            this.diskId = diskId;
	            reload();
	    }
	    public void close() throws IOException {
	           this.input.close();
	           KryoUtil.release(kryo);
	    }

	    public boolean empty() {
	            return (this.cache == null);
	    }

	    public BuyerOrderEntryV23 peek() {
	            return this.cache;
	    }

	    public BuyerOrderEntryV23 pop() throws IOException {
	            BuyerOrderEntryV23 result = this.cache;
	            reload();
	            return result;
	    }

	    private void reload() throws IOException {
	    		if(!input.eof()) this.cache = kryo.readObject(input, BuyerOrderEntryV23.class);
	    		else this.cache = null;
	    }
	   
	   
	    public int getDiskId() {
			return diskId;
		}
	    
		public void setDiskId(int diskId) {
			this.diskId = diskId;
		}
		
		private Input input;
	    
	    private Kryo kryo;

	    private BuyerOrderEntryV23 cache;
	    
	    private int diskId;
	   
	}
	
}
