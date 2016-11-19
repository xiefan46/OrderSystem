package com.alibaba.middleware.race.tree;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.spec.PSource;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.LightRow;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.OrderSecondIndexV13;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.TaskV3;
import com.alibaba.middleware.race.manager.OrderManagerV13;
import com.alibaba.middleware.race.model.Row;

public class OrderTreeV23 {
	
	private String storeRootPath;
	
	private static final String secondIndexFileName = "orderSecondIndexFile";
	
	private ConcurrentSkipListMap<Long,FirstIndex> orderFirstIndexTree
									=new ConcurrentSkipListMap<Long, FirstIndex>();
	
	private RandomAccessFile[] secondIndexRafs;
	
	private String[] rafNames;
	
	private ArrayList<DiskV23> disks;
	
	private static final String dirName = "order_tree_dir/";
	
	private AtomicInteger count = new AtomicInteger(0);
	
	public OrderTreeV23(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void construct(ArrayList<DiskV23> disks) throws Exception
	{
		this.disks = disks;
		mergeSortedOrderIndexFiles(disks);
		constructFirstIndex();
		//三个磁盘均保存索引文件的副本
		//RandomAccessFile raf = new RandomAccessFile(storeRootPath+secondIndexFileName, "r");
		secondIndexRafs = new RandomAccessFile[disks.size()];
		rafNames = new String[disks.size()];
		for(int i=0;i<disks.size();i++)
		{
			DiskV23 disk = disks.get(i);
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
	

	/*
	 * 特别注意这个方法只返回orderid的信息，不会join其他表
	 */
	public LightRow queryOnlyOrderById(long orderid) throws Exception
	{
		SecondIndex secondIndex = querySecondIndex(orderid);
		//System.out.println("二级索引查找到的id为："+secondIndex.getId()+" offset"+secondIndex.getOffset());
		if(secondIndex == null) return null;
		DiskV23 disk = disks.get(secondIndex.getDiskId());
		LightRow row = disk.queryOrder(new OrderSecondIndexV13(secondIndex.getId(), 
				secondIndex.getOffset()));
		return row;
	}
	
	public LightRow queryOnlyOrderByOrderPos(OrderPosInfo posInfo) throws Exception
	{
		DiskV23 disk = disks.get(posInfo.getDiskId());
		LightRow row = disk.queryOrderByPos(posInfo.getOrderpos());
		/*if(row != null) System.out.println("row:"+row);
		else System.out.println("row:null");*/
		return row;
	}
	
	public boolean testOrderIdExist(long orderid) throws Exception
	{
		SecondIndex index = querySecondIndex(orderid);
		if(index != null) return true;
		return false;
	}
	
	private SecondIndex querySecondIndex(long orderid) throws Exception
	{
		int rafId = count.incrementAndGet() % disks.size();
		//System.out.println("本次查询的id："+orderid+"raf id:"+rafId);
		Entry<Long,FirstIndex> entry = orderFirstIndexTree.floorEntry(orderid);
		if(entry == null) {
			System.out.println("查找到subtree为null或者大小为0");
			return null;//orderid不在有效范围
		}
		FirstIndex firstIndex = entry.getValue();
		//System.out.println("first index id："+firstIndex.getId()+" offset:"+firstIndex.getOffset()+" length:"+firstIndex.getLength());
		TaskV3 task = new TaskV3(firstIndex.getOffset(), firstIndex.getLength(), secondIndexRafs[rafId]);
		IOThreadV3 ioThread = disks.get(rafId).getIoThread();
		ioThread.submitTask(task);
		byte[] block = task.getIOResult();
		//System.out.println("成功拿到二级索引byte数据");
		SecondIndex secondIndex = getSecondIndex(block, orderid);
		if(secondIndex == null) 
			return null;  //订单不存在
		return secondIndex;
	}
	
	private void mergeSortedOrderIndexFiles(ArrayList<DiskV23> disks) throws Exception
	{
		File outputFile = new File(storeRootPath + secondIndexFileName);
		if(!outputFile.exists())
			outputFile.createNewFile();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		List<BinaryFileBuffer> buffers = new ArrayList<BinaryFileBuffer>();
		for(int i=0;i<disks.size();i++)
		{
			DiskV23 disk = disks.get(i);
			OrderManagerV13 manager = disk.getOrderManager();
			for(File f : manager.getSortedOrderSmallFile())
			{
				BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f),4096);
	            BinaryFileBuffer bfb = new BinaryFileBuffer(bis,disk.getDiskId());
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
            
            for (BinaryFileBuffer bfb : buffers)
                    if (!bfb.empty())
                            pq.add(bfb);
            try {  
            	//long count = 0;
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	OrderSecondIndexV13 minIndex = bfb.pop();
                	/*count++;
                	if(count % 1000000 == 0){
                		System.out.println("order归并到第："+count+"条");
                	}*/
                	writeOrderSecondIndex(bos, minIndex,bfb.getDiskId()); //新增一个disk id
                    if (bfb.empty()) {
                      bfb.bis.close();
                      pq.remove(bfb);
                     } else {
                      pq.add(bfb); // add it back
                     }
                 }
          
            } finally {
            		bos.flush();
                    bos.close();
                    for (BinaryFileBuffer bfb : pq)
                            bfb.close();
            }
            
    }
	
	private void writeOrderSecondIndex(BufferedOutputStream bos,OrderSecondIndexV13 osi,int diskId) throws Exception
	{
		bos.write(RaceUtil.longToBytes(osi.getId()));
		bos.write(RaceUtil.intToByteArray(diskId));
		bos.write(RaceUtil.longToBytes(osi.getOffset()));
	}
	
	private void constructFirstIndex() throws Exception
	{
		int oneSecondIndexSize = 2*8 + 4;
		int blockSize = FirstIndex.BLOCK_SIZE * oneSecondIndexSize;
		byte[] block = new byte[blockSize];
		File secondIndexFile = new File(storeRootPath + secondIndexFileName);
		if(!secondIndexFile.exists()){
			throw new Exception("二级索引文件不存在");
		}
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(secondIndexFile),4096);
		int count = bis.read(block);
		long curOffset = 0;
		while(count > 0)
		{
			//用每个块中第一个二级索引的orderid来建一级索引
			byte[] orderidInBytes = Arrays.copyOfRange(block, 0, 8);
			long orderid = RaceUtil.bytesToLong(orderidInBytes);
			orderFirstIndexTree.put(orderid, new FirstIndex(orderid, curOffset, count));
			curOffset += count;
			count = bis.read(block);
		}
		bis.close();
	}
	
	private SecondIndex getSecondIndex(byte[] block,long id) throws Exception
	{
		int size = block.length;
		if(size % 20 != 0){
			throw new Exception("对secondIndex解码时发现格式错误");
		}
		int pointer = 0; //一个指向id的指针
		while(pointer < size)
		{
			byte[] idInByte = Arrays.copyOfRange(block, pointer, pointer+8);
			if(RaceUtil.bytesToLong(idInByte) == id){
				byte[] diskIdInByte = Arrays.copyOfRange(block, pointer+8,pointer+12);
				byte[] offsetInByte = Arrays.copyOfRange(block,pointer+12,pointer+20);
				return new SecondIndex(id, RaceUtil.byteArrayToInt(diskIdInByte),
						RaceUtil.bytesToLong(offsetInByte));
			}
			pointer += 20;
		}
		return null;
	}
	
	
	final class BinaryFileBuffer {
	    public BinaryFileBuffer(BufferedInputStream r,int diskId) throws IOException {
	            this.bis = r;
	            this.diskId = diskId;
	            reload();
	    }
	    public void close() throws IOException {
	            this.bis.close();
	    }

	    public boolean empty() {
	            return this.isEmpty;
	    }
	    
	    public int getDiskId()
	    {
	    	return this.diskId;
	    }

	    public OrderSecondIndexV13 peek() {
	            return this.cache;
	    }

	    public OrderSecondIndexV13 pop() throws IOException {
	            OrderSecondIndexV13 result = this.cache;
	            reload();
	            return result;
	    }

	    private void reload() throws IOException {
	    		int size = bis.read(secondIndexInByte);
	    		if(size <= 0) isEmpty = true;
	            this.cache = new OrderSecondIndexV13(secondIndexInByte);
	    }
	    
	    private boolean isEmpty = false;
	    
	    private byte[] secondIndexInByte = new byte[16];

	    public BufferedInputStream bis;

	    private OrderSecondIndexV13 cache;
	    
	    private int diskId;
	    
	}
	

	class FirstIndex{
		public static final int BLOCK_SIZE = 128; //一个一级索引对应约128条二级索引
		
		private long id;
		
		private long offset;
		
		private int length;
		
		public FirstIndex(long id,long offset,int length){
			this.id = id;
			this.offset = offset;
			this.length = length;
		}
		
		
		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}


		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		}
	}
	
	class SecondIndex{
		
		private long id;
		
		private int diskId;
		
		private long offset;

		public SecondIndex(long id,int diskId,long offset)
		{
			this.id = id;
			this.diskId = diskId;
			this.offset = offset;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public int getDiskId() {
			return diskId;
		}

		public void setDiskId(int diskId) {
			this.diskId = diskId;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}
		
		
	}
	
}
