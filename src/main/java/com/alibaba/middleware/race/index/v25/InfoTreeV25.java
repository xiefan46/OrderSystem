package com.alibaba.middleware.race.index.v25;

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
import java.util.HashMap;
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

public class InfoTreeV25 {

	private String primaryKey;
	
	private ArrayList<DiskV25> disks;
	
	private HashMap<String,SecondIndex> indexTree = new HashMap<String,SecondIndex>();
	
	
	private static final long MAX_ENTRY_SIZE = 100*1024*1024/220;
	
	private LoadingCache<String,LightRow> pool = CacheBuilder.newBuilder()
			.concurrencyLevel(15)
			.maximumSize(MAX_ENTRY_SIZE)
			//.recordStats()
			.build(new CacheLoader<String, LightRow>(){
				@Override
				public LightRow load(String id) throws Exception {
					try{
						SecondIndex secondIndex = indexTree.get(id);
						DiskV25 disk = disks.get(secondIndex.getDiskId());
						LightRow row = null;
						if(primaryKey.equals("buyerid")){
							row = disk.queryBuyer(secondIndex.getOffset());
						}
						else if(primaryKey.equals("goodid")){
							row = disk.queryGood(secondIndex.getOffset());
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
	
	
	public InfoTreeV25(String primaryKey)
	{
		this.primaryKey = primaryKey;
	}
	
	public void construct(ArrayList<DiskV25> disks) throws Exception
	{
		//System.out.println("info tree construct");
		this.disks = disks;
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
				indexTree.put(index.getId(), new SecondIndex(index.getOffset(), disk.getDiskId()));	
				line = br.readLine();
			}
			br.close();
		}
	}
	
	public LightRow queryStringIdRecordById(String id) throws Exception
	{
		return pool.get(id);
	}
	
	class SecondIndex{
		private Long offset;
		
		private int diskId;
		
		public SecondIndex(Long offset,int diskId)
		{
			this.offset = offset;
			this.diskId = diskId;
		}

		public Long getOffset() {
			return offset;
		}

	
		public int getDiskId() {
			return diskId;
		}

		
		
	}
	
}
