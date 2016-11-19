package com.alibaba.middleware.race;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.createdata.CreateDataUtil;
import com.alibaba.middleware.race.model.KV;
import com.alibaba.middleware.race.model.ResultImpl;
import com.alibaba.middleware.race.model.Row;

public class RaceUtil {
	
	 public static Row createRowFromRowStr(String rowStr) 
	  {
		    //System.out.println("rowStr:"+rowStr);
		    String[] kvs = rowStr.split("\t");
		    Row kvMap = new Row();
		    for (String rawkv : kvs) {
		      int p = rawkv.indexOf(':');
		      String key = rawkv.substring(0, p);
		      String value = rawkv.substring(p + 1);
		      if (key.length() == 0 || value.length() == 0) {
		        throw new RuntimeException("Bad data:" + rowStr);
		      }
		      KV kv = new KV(key, value);
		      kvMap.put(kv.key(), kv);
		    }
		    return kvMap;
	  }	 
	 
	 public static final String[] keys = {"orderid","goodid","buyerid","createtime"};
	 
	 
	
	 //private static final Map<String,String> map = new HashMap<String, String>();
	 
	 public static Map<String,String> createMapFrmoStr(String line)
	 {
		 Map<String,String> map = new HashMap<String, String>();
		 String[] kvs = line.split("\t");
		 for(String rawkv : kvs)
		 {
			  int p = rawkv.indexOf(':');
		      String key = rawkv.substring(0, p);
		      String value = rawkv.substring(p + 1);
		      if (key.length() == 0 || value.length() == 0) {
			     System.out.println("bad data:" + rawkv);
			  }
		      map.put(key, value);
		 }
		 return map;
	 }
	 
	 
	 
	 
	  
	  public static Collection<File> createFileCollection(Collection<String> fileDir)
	  {
		  Collection<File> files = new HashSet<File>();
		  for(String str : fileDir)
		  {
			  files.add(new File(str));
		  }
		  return files;
	  }
	  
	  /*
		  * 为了方便，暂时把buyerNum和GoodNum相等
		  * 假设orderNum远大于goodNum
		  * 函数属于自已用，别传奇怪的参数进去
		  * 文件命名格式  fileprefix_{iter}_{fileId}
		  * 每个iter生成1000W个goodid和buyerid
		  */
		 public static void createAllTestFiles(int iter)
		 {
			 deleteAndCreateDir();
			 for(int i=0;i<iter;i++) //一次iter生成IdMaxNumInMemory个goodid和buyerid，并生成10-20倍的订单
			 {
				 String[] buyerIds = new String[RaceConfig.IdMaxNumInMemory];
				 String[] goodIds = new String[RaceConfig.IdMaxNumInMemory];
				 for(int j=0;j<RaceConfig.IdMaxNumInMemory;j++)
				 {
					buyerIds[j] = CreateDataUtil.getBuyerId();
					goodIds[j] = CreateDataUtil.getGoodId();
				 }
				 String buyerFileName = RaceConfig.BuyerInfoPath + RaceConfig.BuyerFilePrefix + i;
				 String goodFileName = RaceConfig.GoodInfoPath + RaceConfig.GoodFilePrefix + i;
				 
				 CreateDataUtil.genAllBuyerRecord(buyerFileName,buyerIds); //生成buyer文件
				 CreateDataUtil.genAllGoodRecord(goodFileName,goodIds); //生成good文件
				 //生成order文件(总大小为另外两个文件的25-50倍)
				 for(int j=0;j<5;j++)
				 {
					 String orderFileName = RaceConfig.OrderInfoPath + RaceConfig.OrderFilePrefix + i + "_" +j;
					 int orderNumScale = 5 + (int)(Math.random()*5); //order数量为good和buyer的5-10倍
					 CreateDataUtil.genAllOrderRecord(orderFileName,buyerIds,goodIds,orderNumScale*RaceConfig.IdMaxNumInMemory);
				 }
				 System.out.println("iter : "+i);
				
			 }
		 }
		 
		
		 
		 public static void main(String[] args) throws Exception
		 {
			 createAllTestFiles(1);
			 
			 
			 System.out.println("finish");
		 }
		 
		 private static void deleteAndCreateDir()
		 {
			 deleteAll(new File(RaceConfig.OrderInfoPath));
			 deleteAll(new File(RaceConfig.BuyerInfoPath));
			 deleteAll(new File(RaceConfig.GoodInfoPath));
			 mkDir(new File(RaceConfig.OrderInfoPath));
			 mkDir(new File(RaceConfig.BuyerInfoPath));
			 mkDir(new File(RaceConfig.GoodInfoPath));
		 }
		  
		 public static void mkDir(File file)
		 {
			 
			 if(file.getParentFile().exists()){
				 file.mkdir();
			 }else{
				 mkDir(file.getParentFile());
				 file.mkdir();
			 }
		 }
		 
		 public static void deleteAll(File file)
		 {
			 if(file.exists())
			 {
			  if(file.isFile() || file.list().length == 0)
			  {
			     file.delete();
			  }
			  else
			  {
				  File[] files = file.listFiles();
				  for(File f : files)
				  {	
					 deleteAll(f);//递归删除每一个文件
					 f.delete();//删除该文件夹
				  }
			  }
			 }
		 }
		 
		 public static ArrayList<File> getFilesList(Collection<String> filesDir)
			{
				Collection<File> orderFiles = RaceUtil.createFileCollection(filesDir);
				ArrayList<File> files = new ArrayList<File>();
				for(File f : orderFiles)
				{
					if(f != null && f.exists()){
						files.add(f);
					}
				}
				return files;
			}
		 
		 public static ArrayList<RandomAccessFile> createFilesRafs(ArrayList<File> files) throws Exception
			{
				ArrayList<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();
				for(File f : files)
				{
					System.out.println(f.getName());
					rafs.add(new RandomAccessFile(f, "r"));
				}
				return rafs;
			}
		 
		 public static byte[] intToByteArray(int i) {   
			  byte[] result = new byte[4];   
			  result[0] = (byte)((i >> 24) & 0xFF);
			  result[1] = (byte)((i >> 16) & 0xFF);
			  result[2] = (byte)((i >> 8) & 0xFF); 
			  result[3] = (byte)(i & 0xFF);
			  return result;
			}
		 
		public static int byteArrayToInt(byte[] b) {
			   int offset = 0;
		       int value= 0;
		       for (int i = 0; i < 4; i++) {
		           int shift= (4 - 1 - i) * 8;
		           value +=(b[i + offset] & 0x000000FF) << shift;//往高位游
		       }
		       return value;
		 }
		
	    //private static ByteBuffer buffer = ByteBuffer.allocate(8);  
	    
	    //byte 数组与 long 的相互转换  
	    public static byte[] longToBytes(long x) 
	    {  
	    	 ByteBuffer buffer = ByteBuffer.allocate(8);
	         buffer.putLong(0, x);  
	         return buffer.array();  
	    }  
	      
	    public static long bytesToLong(byte[] bytes) 
	     {  
	    	 ByteBuffer buffer = ByteBuffer.allocate(8);
	         buffer.put(bytes, 0, bytes.length);  
	         buffer.flip();//need flip   
	         return buffer.getLong();  
	     }  
		 
	    public static ArrayList<String> getStringList(Collection<String> strs)
	    {
	    	ArrayList<String> result = new ArrayList<String>();
	    	for(String str : strs)
	    		result.add(str);
	    	return result;
	    		
	    }
	    
	    public static void copyFile(String from,String to) throws IOException{
	    	BufferedInputStream bis = new BufferedInputStream(new FileInputStream(from),4096);
	    	BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(to),4096);
	    	byte[] buffer = new byte[4096]; 
	    	int byteread = 0;
            while ((byteread = bis.read(buffer)) != -1) {  
                bos.write(buffer, 0, byteread);  
            }  
            bis.close();
            bos.flush();
            bos.close();
	    }
	    
	    public static String copyFileToFolder(String from, String toFolder) throws IOException {
	    	File dir = new File(toFolder);
	    	deleteAll(dir);
	    	mkDir(dir);
	        File file =new File( from);
	        copyFile(from, toFolder + file.getName());
	        return toFolder + file.getName();
	    }
	    
	    public static String copyFileToFolderNotDeleteDir(String from, String toFolder) throws IOException {
	        File file =new File( from);
	        copyFile(from, toFolder + file.getName());
	        return toFolder + file.getName();
	    }
	    
	    /*private static final String NOT_EXIST_ID = "NOT_EXIST_SUCH_ID";
	    
	    public static Row getNotExistRow()
	    {
	    	Row row = new Row();
	    	row.putKV(new KV(NOT_EXIST_ID,"null"));
	    	return row;
	    }
	    
	    public static boolean judgeRowNotExist (Row row)
	    {
	    	if(row.get(NOT_EXIST_ID) != null){
	    		return true;
	    	}
	    	return false;
	    }*/
		 
}
