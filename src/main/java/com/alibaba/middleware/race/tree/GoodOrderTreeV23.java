package com.alibaba.middleware.race.tree;

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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.GoodOrderEntryV23;
import com.alibaba.middleware.race.index.model.GoodOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.GoodOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.TaskV3;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.manager.GoodOrderManagerV23;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GoodOrderTreeV23 {
	
	private String storeRootPath;
	
	private ArrayList<DiskV23> disks;
	
	private static final String secondIndexFileName = "secondIndexFile";
	
	private ConcurrentSkipListMap<String, StoreInfo> firstIndex = new ConcurrentSkipListMap<String, StoreInfo>();
	
	private static final String dirName = "good_order_tree_dir/";
	
	private RandomAccessFile[] secondIndexRafs;
	
	private String[] rafNames;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	public GoodOrderTreeV23(String storeRootPath)
	{
		this.storeRootPath = storeRootPath;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void construct(ArrayList<DiskV23> disks) throws Exception
	{
		this.disks = disks;
		mergeSortedSecondIndexFiles();
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
	
	//注意这里得到的只是位置，在外层接口还需要额外对orderid排一次序
	public ArrayList<OrderPosInfo> getOrderPosByGoodid(String goodid) throws Exception
	{
		int rafId = count.incrementAndGet() % disks.size();
		//System.out.println("本次查询的goodid为："+goodid);
		Entry<String,StoreInfo> entry = this.firstIndex.floorEntry(goodid);
		if(entry == null) return null;
		StoreInfo storeInfo = entry.getValue();
		TaskV3 task1 = new TaskV3(storeInfo.getOffset(),storeInfo.getLength(),secondIndexRafs[rafId]);
		disks.get(rafId).getIoThread().submitTask(task1);
		byte[] data = task1.getIOResult();
		Kryo kryo = KryoUtil.borrow();
		Input input = new Input(data);
		GoodOrderFirstIndexV23 block = kryo.readObject(input, GoodOrderFirstIndexV23.class);
		input.close();
		KryoUtil.release(kryo);
		//System.out.println("block id 为："+block.getId());
		GoodOrderSecondIndexV23 map = block.find(goodid);
		if(map == null) return null;
		ArrayList<OrderPosInfo> orderPos = map.getItems();
		return orderPos;
	}

	private void mergeSortedSecondIndexFiles() throws Exception
	{
		File outputFile = new File(storeRootPath + secondIndexFileName);
		if(!outputFile.exists())
			outputFile.createNewFile();
		//BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		List<BinaryFileBuffer> buffers = new ArrayList<BinaryFileBuffer>();
		for(int i=0;i<disks.size();i++)
		{
			DiskV23 disk = disks.get(i);
			GoodOrderManagerV23 manager = disk.getGoodOrderManager();
			for(File f : manager.getSortedGoodOrderSmallFile())
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
            GoodOrderSecondIndexV23 map = new GoodOrderSecondIndexV23();
            GoodOrderFirstIndexV23 goodBlock = new GoodOrderFirstIndexV23();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Kryo kryo = KryoUtil.borrow();
            Output output = new Output(baos);
            long curOffset = 0;
            //long count = 1;
            try {                        
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	GoodOrderEntryV23 minEntry = bfb.pop();
                	/*count++;
                	
                	if(count % 1000000 == 0){
                		System.out.println("good order归并到第："+count+"条");
                	}*/
                	if(map.getGoodid() == null || map.getGoodid().equals(minEntry.getGoodid())){
                		map.addEntry(minEntry,bfb.getDiskId());
                	}else {
                		goodBlock.putBlock(map);
                		map = new GoodOrderSecondIndexV23();
                		map.addEntry(minEntry,bfb.getDiskId());
                	}
                	//写入二级索引并同时在上面建立一级索引
                	if(goodBlock.isFull())
                	{
                		kryo.writeObject(output, goodBlock);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		this.firstIndex.put(goodBlock.getId(), new StoreInfo(curOffset, length));
                		curOffset += length;
                		bos.write(data);
                		baos.reset();
                		goodBlock = new GoodOrderFirstIndexV23();
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
            	 		goodBlock.putBlock(map);
         	    	}
            		if(!goodBlock.isEmpty()){
            			kryo.writeObject(output, goodBlock);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		this.firstIndex.put(goodBlock.getId(), new StoreInfo(curOffset, length));
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
            //System.out.println("归并完成");
            
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

	    public GoodOrderEntryV23 peek() {
	            return this.cache;
	    }

	    public GoodOrderEntryV23 pop() throws IOException {
	    		GoodOrderEntryV23 result = this.cache;
	            reload();
	            return result;
	    }

	    private void reload() throws IOException {
	    		if(!input.eof()) this.cache = kryo.readObject(input, GoodOrderEntryV23.class);
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

	    private GoodOrderEntryV23 cache;
	    
	    private int diskId;
	    
	}
	
	
}
