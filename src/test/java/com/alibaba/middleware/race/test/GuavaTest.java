
package com.alibaba.middleware.race.test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
  
public class GuavaTest {
	
	@org.junit.Test
	public void testCache1() throws Exception
	{
		LoadingCache<CacheKey,String> cacheBuilder = CacheBuilder.newBuilder()
				.build(new CacheLoader<CacheKey, String>(){

					@Override
					public String load(CacheKey key) throws Exception {
						System.out.println("缓存缺失");
						return null;
					}								
				});
       CacheKey k1 = new CacheKey(12306,"buyername");
       String v1 = "张三";
       CacheKey k2 = new CacheKey(12306,"buyername");
       cacheBuilder.put(k1, v1);
       System.out.println(k1.equals(k2));
       System.out.println(cacheBuilder.get(k2));
		
	}
}

class CacheKey
{
	private final Long orderId;
	
	private final String key;
	
	public CacheKey(long orderId,String key){
		this.orderId = orderId;
		this.key = key;
	}

	public long getOrderId() {
		return orderId;
	}

	public String getKey() {
		return key;
	}
	
	@Override
	public boolean equals(Object other)
	{
		CacheKey otherKey = (CacheKey)other;
		if(this.orderId.equals(otherKey.orderId) && this.key.equals(otherKey.key)) return true;
		return false;
	}
	
	public boolean equals(CacheKey otherKey)
	{
		if(this.orderId.equals(otherKey.orderId)&& this.key.equals(otherKey.key)) return true;
		return false;
	}
	
	@Override
	public int hashCode()
	{
		return orderId.hashCode() ;
	}
		
}
