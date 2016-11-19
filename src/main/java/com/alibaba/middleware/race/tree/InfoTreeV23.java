package com.alibaba.middleware.race.tree;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

import javax.naming.ldap.ManageReferralControl;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.LightRow;
import com.alibaba.middleware.race.index.model.StringSecondIndexV13;
import com.alibaba.middleware.race.io.DiskV23;
import com.alibaba.middleware.race.io.IOThreadV3;
import com.alibaba.middleware.race.io.TaskV3;
import com.alibaba.middleware.race.manager.InfoManagerV13;
import com.alibaba.middleware.race.model.Row;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class InfoTreeV23 {
	
	private String storeRootPath;
	
	private String primaryKey;
	
	private ArrayList<DiskV23> disks;
	
	private static final String secondIndexFileName = "SecondIndexFile";
	
	private final String dirName;
	
	private StringFirstIndexTreeV23 firstIndexTree;
	
	private static final long MAX_ENTRY_SIZE = 100*1024*1024/220;
	
	private LoadingCache<String,LightRow> pool = CacheBuilder.newBuilder()
			.concurrencyLevel(15)
			.maximumSize(MAX_ENTRY_SIZE)
			//.recordStats()
			.build(new CacheLoader<String, LightRow>(){
				@Override
				public LightRow load(String id) throws Exception {
					try{
						byte[] block = firstIndexTree.querySecondIndexDataById(id);
						if(block == null){
							System.out.println("id不存在");
							return null;
						}
						SecondIndex secondIndex = getSecondIndex(block, id);
						if(secondIndex == null) {
							System.out.println("id不存在");
							return null;  
						}
						/*System.out.println("二级索引查找到的id为："+secondIndex.getSsi().getId()+
								" fileid:"+secondIndex.getSsi().getFileId()+" offset"+secondIndex.getSsi().getOffset()
								+" length:"+secondIndex.getSsi().getLength());*/
						DiskV23 disk = disks.get(secondIndex.getDiskId());
						LightRow row = null;
						if(primaryKey.equals("buyerid")){
							row = disk.queryBuyer(secondIndex.getSsi());
						}
						else if(primaryKey.equals("goodid")){
							row = disk.queryGood(secondIndex.getSsi());
						}else{
							throw new Exception("存在不合法primary key");
						}
						if(row == null)
							System.out.println("null");
						return row;
					}catch(Exception e){
						return null;
					}
				}								
			});
	
	
	public InfoTreeV23(String storeRootPath,String primaryKey,String dirName)
	{
		this.storeRootPath = storeRootPath;
		this.primaryKey = primaryKey;
		this.dirName = dirName;
		File dir = new File(storeRootPath);
		RaceUtil.deleteAll(dir);
		RaceUtil.mkDir(dir);
	}
	
	public void construct(ArrayList<DiskV23> disks) throws Exception
	{
		System.out.println("info tree construct");
		this.disks = disks;
		mergeSecondIndexSmallFiles();	
		String[] rafNames = new String[disks.size()];
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
		this.firstIndexTree = new StringFirstIndexTreeV23(rafNames,disks,storeRootPath, secondIndexFileName);
	}
	
	public LightRow queryStringIdRecordById(String id) throws Exception
	{
		
		return pool.get(id);
	}
	
	private void mergeSecondIndexSmallFiles() throws Exception
	{
		File outputFile = new File(storeRootPath + secondIndexFileName);
		if(!outputFile.exists())
			outputFile.createNewFile();
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)),4096);
		List<BinaryFileBuffer> buffers = new ArrayList<BinaryFileBuffer>();
		for(int i=0;i<disks.size();i++)
		{
			DiskV23 disk = disks.get(i);
			InfoManagerV13 manager;
			if(primaryKey.equals("buyerid")){
				manager = disk.getBuyerManager();
			}else if(primaryKey.equals("goodid")){
				manager = disk.getGoodManager();
			}else{
				throw new Exception("存在不合法primary key");
			}
			for(File f : manager.getSortedSecondSmallFile())
			{
				 	BufferedReader br = new BufferedReader(new InputStreamReader(
		            		new FileInputStream(f)),4096);
		            BinaryFileBuffer bfb = new BinaryFileBuffer(br,disk.getDiskId());
		            buffers.add(bfb);
			}
		}
		mergeSortedFiles(bw, buffers);
	}
	
	private void mergeSortedFiles(BufferedWriter bw,List<BinaryFileBuffer> buffers) throws Exception 
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
                while (pq.size() > 0) 
                {
                	BinaryFileBuffer bfb = pq.poll();
                	StringSecondIndexV13 minIndex = bfb.pop();
                	writeSecondIndex(bw, minIndex,bfb.getDiskId());
                    if (bfb.empty()) {
                      bfb.br.close();
                      pq.remove(bfb);
                     } else {
                      pq.add(bfb); // add it back
                     }
                 }
          
            } finally {
            		bw.flush();
                    bw.close();
                    for (BinaryFileBuffer bfb : pq)
                            bfb.close();
            }
            
    }
	
	private void writeSecondIndex(BufferedWriter bw ,StringSecondIndexV13 ssi,int diskId) throws Exception
	{
		bw.write(ssi.toString());
		bw.write("\t"+diskId);
		bw.write("\n");
	}
	
	private SecondIndex getSecondIndex(byte[] block,String id) throws Exception
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(block)),4096);
		String line = br.readLine();
		while(line != null)
		{
			String strs[] = line.split("\t");
			String id_ = strs[0];
			if(id_.equals(id)){
				long offset = Long.parseLong(strs[1]);
				int diskId = Integer.parseInt(strs[2]);
				return new SecondIndex(new StringSecondIndexV13(id_,offset), diskId);
			}
			line = br.readLine();
		}
		return null;
	}
	
	
	final class BinaryFileBuffer {
	    public BinaryFileBuffer(BufferedReader r,int diskId) throws IOException {
	            this.br = r;
	            this.diskId = diskId;
	            reload();
	    }
	    public void close() throws IOException {
	            this.br.close();
	    }

	    public boolean empty() {
	            return this.isEmpty;
	    }

	    public StringSecondIndexV13 peek() {
	            return this.cache;
	    }
	    
	    public int getDiskId(){
	    	return this.diskId;
	    }

	    public StringSecondIndexV13 pop() throws IOException {
	            StringSecondIndexV13 result = this.cache;
	            reload();
	            return result;
	    }

	    private void reload() throws IOException {
	    		//int size = bw.read(secondIndexInByte);
	    		String line = br.readLine();
	    		if(line == null) {
	    			isEmpty = true;
	    			this.cache = null;
	    		}
	    		else {
	    			this.cache = new StringSecondIndexV13(line);
	    		}
	    }
	    
	    private boolean isEmpty = false;

	    public BufferedReader br;

	    private StringSecondIndexV13 cache;
	    
	    private int diskId;
	}
	
	class SecondIndex{
		private StringSecondIndexV13 ssi;
		
		private int diskId;
		
		public SecondIndex(StringSecondIndexV13 ssi,int diskId)
		{
			this.ssi = ssi;
			this.diskId = diskId;
		}

		public StringSecondIndexV13 getSsi() {
			return ssi;
		}

		public void setSsi(StringSecondIndexV13 ssi) {
			this.ssi = ssi;
		}

		public int getDiskId() {
			return diskId;
		}

		public void setDiskId(int diskId) {
			this.diskId = diskId;
		}
		
	}
	
}
