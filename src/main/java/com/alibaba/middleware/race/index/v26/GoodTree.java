package com.alibaba.middleware.race.index.v26;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.alibaba.middleware.race.index.model.BuyerOrderEntryV23;
import com.alibaba.middleware.race.index.model.BuyerOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.BuyerOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.GoodOrderEntryV23;
import com.alibaba.middleware.race.index.model.GoodOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.GoodOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.index.v25.DiskV25;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.manager.BuyerOrderManagerV23;
import com.alibaba.middleware.race.manager.GoodOrderManagerV23;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GoodTree extends InfoTreeV26{
	
	public GoodTree(String primaryKey, ArrayList<DiskV25> disks,String storeRootPath,String dirName) {
		super(primaryKey, disks,storeRootPath,dirName);
	}

	@Override
	protected void scanOrderInfo() {
		try {
			mergeSortedSecondIndexFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void mergeSortedSecondIndexFiles() throws Exception
	{
		File outputFile = new File(storeRootPath + OrderInfoIndexFileName);
		if(!outputFile.exists())
			outputFile.createNewFile();
		//BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(outputFile),4096);
		List<BinaryFileBuffer> buffers = new ArrayList<BinaryFileBuffer>();
		for(int i=0;i<disks.size();i++)
		{
			DiskV25 disk = disks.get(i);
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
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Kryo kryo = KryoUtil.borrow();
            Output output = new Output(baos);
            long curOffset = 0;
            try {                        
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	GoodOrderEntryV23 minEntry = bfb.pop();
                	if(map.getGoodid() == null || map.getGoodid().equals(minEntry.getGoodid())){
                		map.addEntry(minEntry,bfb.getDiskId());
                	}else {
                		kryo.writeObject(output, map);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		TreeStoreInfo info = indexTree.get(map.getGoodid());
                		StoreInfo oInfo = new StoreInfo(curOffset, length);
                		info.setOrderInfo(oInfo);
                		curOffset += length;
                		bos.write(data);
                		baos.reset();
                		map = new GoodOrderSecondIndexV23();
                		map.addEntry(minEntry,bfb.getDiskId());
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
            	 		kryo.writeObject(output, map);
                		output.flush();
                		byte[] data = baos.toByteArray();
                		int length = data.length;
                		TreeStoreInfo info = indexTree.get(map.getGoodid());
                		StoreInfo oInfo = new StoreInfo(curOffset, length);
                		info.setOrderInfo(oInfo);
                		curOffset += length;
                		bos.write(data);
                		baos.reset();
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
