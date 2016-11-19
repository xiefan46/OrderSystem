package com.alibaba.middleware.race.kryo;

import java.util.ArrayList;

import com.alibaba.middleware.race.index.model.BuyerOrderEntryV23;
import com.alibaba.middleware.race.index.model.BuyerOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.BuyerOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.GoodOrderEntryV23;
import com.alibaba.middleware.race.index.model.GoodOrderFirstIndexV23;
import com.alibaba.middleware.race.index.model.GoodOrderSecondIndexV23;
import com.alibaba.middleware.race.index.model.OrderPosInfo;
import com.alibaba.middleware.race.index.model.OrderposTime;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

public class KryoUtil {
	
	private static KryoFactory factory = new KryoFactory() {
		
		@Override
		public Kryo create() {
			Kryo kryo = new Kryo();
			//用于Buyer Order Map
			/*kryo.register(OrderIdTime.class,0);
			kryo.register(BuyerOrderEntry.class,1);
			kryo.register(BuyerOrderSecondIndex.class,2);
			kryo.register(ArrayList.class, 3);
			kryo.register(BuyerOrderFirstIndex.class,4);
			//用于GoodOrderMap
			kryo.register(GoodOrderEntry.class, 5);
			kryo.register(GoodOrderFirstIndex.class,6);
			kryo.register(GoodOrderSecondIndex.class, 7);*/
			kryo.register(ArrayList.class, 0);
			kryo.register(OrderPosInfo.class, 1);
			kryo.register(OrderposTime.class,2);
			kryo.register(BuyerOrderEntryV23.class,3);
			kryo.register(GoodOrderEntryV23.class,4);
			kryo.register(BuyerOrderFirstIndexV23.class, 5);
			kryo.register(BuyerOrderSecondIndexV23.class, 6);
			kryo.register(GoodOrderFirstIndexV23.class, 7);
			kryo.register(GoodOrderSecondIndexV23.class, 8);
			
			
			
			return kryo;
		}
	};
	
	private static KryoPool pool = new KryoPool.Builder(factory).build();
	
	public static Kryo borrow()
	{
		return pool.borrow();
	}
	
	public static void release(Kryo kryo)
	{
		pool.release(kryo);
	}
	
}
