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
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.index.v25.DiskV25;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.kryo.KryoUtil;
import com.alibaba.middleware.race.manager.BuyerOrderManagerV23;
import com.alibaba.middleware.race.tree.BuyerOrderTreeV23.BinaryFileBuffer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BuyerTree extends InfoTreeV26{

	public BuyerTree(String primaryKey, ArrayList<DiskV25> disks,
			String storeRootPath, String dirName) {
		super(primaryKey, disks, storeRootPath, dirName);
		
	}

	@Override
	protected void scanOrderInfo() {
		
	}
	
	private void mergeSortedSecondIndexFiles() throws Exception
	{
		File outputFile = new File(storeRootPath + OrderInfoIndexFileName);
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
