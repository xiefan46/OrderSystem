package com.alibaba.middleware.race.config;

public interface RaceConfig {
	//存储的路径
	public static final String StoreRootPath = "../middlewareStore/";
		
	public static final String OrderInfoPath = StoreRootPath + "orderFiles/";
		
	public static final String GoodInfoPath = StoreRootPath + "goodFiles/";
		
	public static final String BuyerInfoPath = StoreRootPath + "buyerFiles/";
		
	public static final String OrderFilePrefix = "order_file_";
		
	public static final String GoodFilePrefix = "good_file_";
		
	public static final String BuyerFilePrefix = "buyer_file_";
	
	public static final String OrderIndexPath = StoreRootPath + "orderIndexFiles/";
	
	public static final String OrderIndexFileName = "orderIndexFile";
	
	public static final String GoodIndexPath = StoreRootPath + "goodIndexFiles/";
	
	public static final String GoodIndexFileName = "goodIndexFile";
	
	public static final String BuyerIndexPath = StoreRootPath + "buyerIndexFiles/";

	public static final String BuyerIndexFileName = "buyerIndexFile";
	/*
	 * 防止爆内存的一些变量
	* 一个id假设占64B，则在内存中保存1000W个id大约需要640MB
	* 注意的是orderid同时保存在内存中需要额外预留20倍的内存
	* 需要同时保存goodid和buyerid才能生成order信息  .
	*/
	public static final int IdMaxNumInMemory = 1000;
	
	
	public enum ExistenceType{
		EXIST,NOT_SURE,NOT_EXIST
	}
	
	public enum QueryFileType{
		ORDER,BUYER,GOOD
	}
	
	public static final String VALUE_NOT_EXIST = "value_no_exist#";//用于标记不存在的字段
	
	
}
