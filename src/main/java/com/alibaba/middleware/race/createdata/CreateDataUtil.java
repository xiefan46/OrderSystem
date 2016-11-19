package com.alibaba.middleware.race.createdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/*
 * 一个按照一定格式生成测试数据的类
 */
public class CreateDataUtil 
{
	//private static final String StoreDataPath = "../store";
	
	private static final long seed = 1234;
	
	public static Random rand = new Random(seed);;
	
	private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz" +
			"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();
	
	/*
	 * 各种id(buyer、good、saler)都有一些固定的前缀
	 */
	private static String[] buyerIdPrefix = {"ap_","tb_"};
	
	private static String[] goodIdPrefix = {"good_","aliyun_","goodtb_","goodxn_","goodal_"};
	
	private static String[] salerIdPrefix = {"almm_","wx_","tb_","tm_"};
	
	//order 暂时按序生成,再打乱
	private static long orderid = 20000;
	
	/*
	 * 用于保存goodid和salerid的表
	 * 需要预先生成好good_record文件和buyer_record文件
	 */
	private List<String> goodIdTable = null;
	
	private List<String> buyerIdTable = null;
	
	private static final String beginDate = "2010-09-01";
	
	private static final String endDate = "2016-07-01";
	
	
	private static DecimalFormat df = new DecimalFormat("0.##");
	
	public static String getUUID()
	{
	    UUID uuid=UUID.randomUUID();
	    String str = uuid.toString(); 
	    return str;
	  }
	
	public static String getRamdonString()
	{
		int length = numbersAndLetters.length;
		char [] randBuffer = new char[length];
		for (int i=0; i<randBuffer.length; i++) {
			randBuffer[i] = numbersAndLetters[rand.nextInt(length)];
		}
		return new String(randBuffer);
	}
	
	
	/*
	 * TODO 一些出现次数不定的属性还没有生成
	 * TODO good_record中暂定goodid构成主码
	 */
	/*
	public static void genAllData(int buyerNum,int goodNum,int orderNum)
	{
		String[] buyerIds = new String[buyerNum];
		String[] goodIds = new String[goodNum];
		for(int i=0;i<buyerNum;i++)
		{
			buyerIds[i] = buyerIdPrefix[rand.nextInt(buyerIdPrefix.length)] + getUUID();
			goodIds[i] = goodIdPrefix[rand.nextInt(goodIdPrefix.length)] + getUUID();
		}
		genAllBuyerRecord("my_buyer_record.txt",buyerIds);
		genAllGoodRecord("my_good_record.txt",goodIds);
		genAllOrderRecord("my_order_record.txt",buyerIds,goodIds,orderNum);
	}*/
	
	
	public static long getRandomTime(String beginDate,String endDate) throws Exception
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date start = format.parse(beginDate);
		Date end = format.parse(endDate);
		if(start.getTime() >= end.getTime()) {
			return -1;
		}
		Float f = new Float(rand.nextFloat()*(end.getTime()-start.getTime()));
		long result = start.getTime() + f.longValue();
		return result;
	}
	
	public static String getBuyerId()
	{
		return buyerIdPrefix[rand.nextInt(buyerIdPrefix.length)] + getUUID();
	}
	
	public static String getGoodId()
	{
		return goodIdPrefix[rand.nextInt(goodIdPrefix.length)] + getUUID();
	}
	
	public static void genAllBuyerRecord(String fileName,String[] buyerIds)
	{
		try{
			
			File buyerRecordFile = new File(fileName);
		    if(!buyerRecordFile.exists())
		    {
		    	buyerRecordFile.createNewFile();
		    }
		    BufferedWriter bufferWritter = new BufferedWriter(new FileWriter(buyerRecordFile));
		    for(int i=0;i<buyerIds.length;i++)
		    {
		    	String record = getOneBuyerRecord(buyerIds[i]);
		    	bufferWritter.write(record+"\n");
		    }
		    bufferWritter.flush();
		    bufferWritter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void genAllGoodRecord(String fileName,String[] goodIds)
	{
		try{
			
			File buyerRecordFile = new File(fileName);
		    if(!buyerRecordFile.exists())
		    {
		    	buyerRecordFile.createNewFile();
		    }
		    BufferedWriter bufferWritter = new BufferedWriter(new FileWriter(buyerRecordFile));
		    int size = goodIds.length / 5;
		    String[] salerIds = new String[size];  //saler假设为good的5分之一
		    for(int i=0;i<size;i++)
		    {
		    	salerIds[i] = salerIdPrefix[rand.nextInt(salerIdPrefix.length)] + getUUID();
		    }
		    for(int i=0;i<goodIds.length;i++)
		    {
		    	String record = getOneGoodRecord(goodIds[i],salerIds);
		    	//System.out.println("record  "+record);
		    	bufferWritter.write(record+"\n");
		    }
		    bufferWritter.flush();
		    bufferWritter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static String getOneBuyerRecord(String buyerId)
	{
		StringBuilder resultRecord = new StringBuilder();
		resultRecord.append("buyerid:"+buyerId);
		//生成contactphone
		int tmp = rand.nextInt(50);
		if(tmp < 45) //约十分之一的用户地址为空
		{
			resultRecord.append("\t"+"contactphone:"+getRamdonString());
		}
		//生成buyername
		tmp = rand.nextInt(50);
		if(tmp < 45)
		{
			resultRecord.append("\t"+"buyername:"+getRamdonString());
		}
		//生成receiveaddress
		tmp = rand.nextInt(50);
		if(tmp < 45)
		{
			resultRecord.append("\t"+"receiveaddress:"+getRamdonString());
		}
		//System.out.println(resultRecord.toString());
		resultRecord.append("\t");
		resultRecord.append(getRandomAttrString("app_buyer"));
		return resultRecord.toString();
	}
	
	private static String getOneGoodRecord(String goodId,String[] salerIds)
	{
		StringBuilder resultRecord = new StringBuilder();
		StringBuilder salerid = new StringBuilder();
		resultRecord.append("goodid:"+goodId);
		/*int tmp = rand.nextInt(salerIdPrefix.length);
		salerid.append(salerIdPrefix[tmp]);
		salerid.append(getUUID());*/
		salerid.append(salerIds[rand.nextInt(salerIds.length)]);
		resultRecord.append("\t"+"salerid:"+salerid.toString());
		//生成good_name
		int tmp = rand.nextInt(50);
		if(tmp < 45) 
		{
			resultRecord.append("\t"+"good_name:"+getRamdonString());
		}
		//生成price
		tmp = rand.nextInt(50);
		if(tmp < 45) 
		{
			resultRecord.append("\t"+"price:"+df.format(100*rand.nextDouble()));
		}
		//生成offprice
		tmp = rand.nextInt(50);
		if(tmp < 45) 
		{
			resultRecord.append("\t"+"offprice:"+df.format(100*rand.nextDouble()));
		}
		//生成goodname
		tmp = rand.nextInt(50);
		if(tmp < 45) 
		{
			resultRecord.append("\t"+"goodname:"+getRamdonString());
		}
		//System.out.println(resultRecord.toString());
		resultRecord.append("\t");
		resultRecord.append(getRandomAttrString("app_good"));
		return resultRecord.toString();
	}
	
	public static void genAllOrderRecord(String fileName,String[] buyerIds,String[] goodIds,int orderNum)
	{
		try{
			
			File orderRecordFile = new File(fileName);
		    if(!orderRecordFile.exists())
		    {
		    	orderRecordFile.createNewFile();
		    }
		    BufferedWriter bufferWritter = new BufferedWriter(new FileWriter(orderRecordFile));
		    //先生成所有orderid然后再打乱顺序
			List<Long> orderIds = new ArrayList<Long>();
			for(int i=0;i<orderNum;i++)
			{
				orderIds.add(orderid++);
			}
			Collections.shuffle(orderIds);
			//开始生成订单记录
		    for(int i=0;i<orderNum;i++)
		    {
		    	
		    	String record = getOneOrderRecord(orderIds.get(i),goodIds[rand.nextInt(goodIds.length)]
		    									,buyerIds[rand.nextInt(buyerIds.length)]);
		    	//System.out.println("record  "+record);
		    	bufferWritter.write(record+"\n");
		    }
		    bufferWritter.flush();
		    bufferWritter.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/*
	 * 
	 */
	private static String getOneOrderRecord(long orderId,String goodId,String buyerId) throws Exception
	{
		StringBuilder resultRecord = new StringBuilder();
		resultRecord.append("orderid:"+orderId);
		resultRecord.append("\t"+"goodid:"+goodId);
		resultRecord.append("\t"+"buyerid:"+buyerId);
		resultRecord.append("\t"+"createtime:"+getRandomTime(beginDate, endDate));
		int tmp = rand.nextInt(10);
		if(tmp != 0)	resultRecord.append("\tdone:true");
		else resultRecord.append("\tdone:false");
		//TODO amount这样生成是否符合规范
		tmp = rand.nextInt(10);
		if(tmp != 0) resultRecord.append("\tamount:"+rand.nextInt(1000));
		tmp = rand.nextInt(10);
		if(tmp == 0) resultRecord.append("\tremark:"+getRamdonString());
		resultRecord.append("\t");
		resultRecord.append(getRandomAttrString("app_order"));
		return resultRecord.toString();
	}
	
	/*
	 * 三个记录文件中都有一些不定名称的稀有属性，这个函数的作用是生成这些属性
	 * 格式 attrPrefix_{0-30}_{0-30}:{random num}
	 */
	private static String getRandomAttrString(String attrPrefix)
	{
		int attrNum = rand.nextInt(10);
		if(attrNum == 0) return "";
		HashSet<String> attrSet = new HashSet<String>();
		for(int i=0;i<attrNum;i++)
		{
			String str = getOneAttrString(attrPrefix);
			while(attrSet.contains(str))
				str = getOneAttrString(attrPrefix);
			attrSet.add(str);
		}
		StringBuilder sb = new StringBuilder();
		for(String attr : attrSet)
		{
			sb.append(attr+":"+df.format(10000*rand.nextDouble()) +"\t");
		}
		return sb.toString();
	}
	
	private static String getOneAttrString(String attrPrefix)
	{
		return attrPrefix + "_" + rand.nextInt(31)+"_"+rand.nextInt(31);
	}
	
	/*public static void main(String[] args) throws Exception
	{
		genAllData(500, 500, 10000);
	}*/
}
