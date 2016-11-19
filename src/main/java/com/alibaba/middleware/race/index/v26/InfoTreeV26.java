package com.alibaba.middleware.race.index.v26;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeMap;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.index.model.StoreInfo;
import com.alibaba.middleware.race.index.model.StringSecondIndexV13;
import com.alibaba.middleware.race.index.v25.DiskV25;
import com.alibaba.middleware.race.index.v25.InfoManagerV25;
import com.alibaba.middleware.race.io.DiskV23;


/*
 * 将goodTree和goodOrderTree合并，buyerTree和buyerOrderTree合并
 */
public abstract class InfoTreeV26 {
	
	protected String primaryKey;
	
	protected ArrayList<DiskV25> disks;
	
	protected TreeMap<String,TreeStoreInfo> indexTree = new TreeMap<String,TreeStoreInfo>();
	
	protected String storeRootPath;
	
	protected final String OrderInfoIndexFileName = "OrderInfoIndex";
	
	protected RandomAccessFile[] secondIndexRafs;
	
	private String[] rafNames;
	
	private String dirName;
	
	public InfoTreeV26(String primaryKey,ArrayList<DiskV25> disks,String storeRootPath,String dirName)
	{
		this.primaryKey = primaryKey;
		this.disks = disks;
		this.storeRootPath = storeRootPath;
		this.dirName = dirName;
	}
	
	public void construct() throws Exception
	{
		scanInfoSecondIndex();
		scanOrderInfo();
		crossCopyOrderInfoIndex();
	}
	
	public void scanInfoSecondIndex() throws Exception
	{
		for(int i=0;i<disks.size();i++)
		{
			DiskV25 disk = disks.get(i);
			InfoManagerV25 manager;
			if(primaryKey.equals("buyerid")){
				manager = disk.getBuyerManager();
			}
			else if(primaryKey.equals("goodid")){
				manager = disk.getGoodManager();
			}else{
				throw new Exception("info tree中存在不合法primary key");
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(manager.getSecondIndexFile())),4096);
			String line = br.readLine();
			while(line != null)
			{
				//System.out.println("line:"+line);
				StringSecondIndexV13 index = new StringSecondIndexV13(line);
				TreeStoreInfo info = new TreeStoreInfo();
				info.setPosInfo(new PosInfo(index.getOffset(),disk.getDiskId()));
				indexTree.put(index.getId(), info);
				line = br.readLine();
			}
			br.close();
		}
	}
	
	protected abstract void scanOrderInfo();
	
	private void crossCopyOrderInfoIndex() throws Exception
	{
		secondIndexRafs = new RandomAccessFile[disks.size()];
		rafNames = new String[disks.size()];
		for(int i=0;i<disks.size();i++)
		{
			DiskV25 disk = disks.get(i);
			String name = disk.getStoreRootPath()+dirName;
			if(name.equals(this.storeRootPath))
				rafNames[i] = name + OrderInfoIndexFileName;
			else{
				String fileName = RaceUtil.copyFileToFolder(storeRootPath+OrderInfoIndexFileName,name);
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
}

class PosInfo{
	private long offset;
	
	private int diskId;

	public PosInfo(long offset, int diskId) {
		super();
		this.offset = offset;
		this.diskId = diskId;
	}
	
	
}

class TreeStoreInfo{
		private PosInfo posInfo; //buyer或者good信息的位置
	  
		private StoreInfo orderInfo; //对应的order集合的位置
		
		public TreeStoreInfo(){
			
		}

		public PosInfo getPosInfo() {
			return posInfo;
		}

		public void setPosInfo(PosInfo posInfo) {
			this.posInfo = posInfo;
		}

		public StoreInfo getOrderInfo() {
			return orderInfo;
		}

		public void setOrderInfo(StoreInfo orderInfo) {
			this.orderInfo = orderInfo;
		}
		
		
}
