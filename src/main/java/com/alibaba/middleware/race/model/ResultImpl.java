package com.alibaba.middleware.race.model;

import java.util.Set;
import java.util.Map.Entry;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.index.model.LightRow;


public class ResultImpl implements Result {
    private long orderid;
    private Row kvMap;

    public ResultImpl(long orderid, Row kv) {
      this.orderid = orderid;
      this.kvMap = kv;
    }
    
    static public ResultImpl createResultRow(LightRow orderData, Set<String> queryingKeys) {
        if (orderData == null ) {
      	System.out.println("bad data:");
      	System.out.println(orderData);
          throw new RuntimeException("Bad data!");
        }
        Row allkv = new Row();
        long orderid;
        try {
          orderid = Long.parseLong(orderData.get("orderid"));
        } catch (Exception e) {
          throw new RuntimeException("Bad data!");
        }

        for (Entry<String,String> kv : orderData.entrySet()) {
          if (queryingKeys == null || queryingKeys.contains(kv.getKey())) {
            allkv.putKV(kv.getKey(),kv.getValue());
          }
        }
        return new ResultImpl(orderid, allkv);
   }
    
    static public ResultImpl createResultRow(Row orderData, Set<String> queryingKeys) {
          if (orderData == null ) {
        	System.out.println("bad data:");
        	System.out.println(orderData);
            throw new RuntimeException("Bad data!");
          }
          Row allkv = new Row();
          long orderid;
          try {
            orderid = orderData.get("orderid").valueAsLong();
          } catch (TypeException e) {
            throw new RuntimeException("Bad data!");
          }

          for (KV kv : orderData.values()) {
            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
              allkv.put(kv.key(), kv);
            }
          }
          return new ResultImpl(orderid, allkv);
     }
    
    static public ResultImpl createResultRow(Row orderData, Row buyerData, Set<String> queryingKeys) {
          if (orderData == null || buyerData == null ) {
        	System.out.println("bad data:");
        	System.out.println(orderData);
        	System.out.println(buyerData);
            throw new RuntimeException("Bad data!");
          }
          Row allkv = new Row();
          long orderid;
          try {
            orderid = orderData.get("orderid").valueAsLong();
          } catch (TypeException e) {
            throw new RuntimeException("Bad data!");
          }

          for (KV kv : orderData.values()) {
            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
              allkv.put(kv.key(), kv);
            }
          }
          for (KV kv : buyerData.values()) {
            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
              allkv.put(kv.key(), kv);
            }
          }
          return new ResultImpl(orderid, allkv);
        }
    
    static public ResultImpl createResultRow(Row orderData, Row buyerData,
        Row goodData, Set<String> queryingKeys) {
      if (orderData == null || buyerData == null || goodData == null) {
    	System.out.println("bad data:");
    	System.out.println(orderData);
    	System.out.println(buyerData);
    	System.out.println(goodData);
        throw new RuntimeException("Bad data!");
      }
      Row allkv = new Row();
      long orderid;
      try {
        orderid = orderData.get("orderid").valueAsLong();
      } catch (TypeException e) {
        throw new RuntimeException("Bad data!");
      }

      for (KV kv : orderData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : buyerData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : goodData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      return new ResultImpl(orderid, allkv);
    }

    public KeyValue get(String key) {
      return this.kvMap.get(key);
    }

    public KeyValue[] getAll() {
      return kvMap.values().toArray(new KeyValue[0]);
    }

    public long orderId() {
      return orderid;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("orderid: " + orderid + " {");
      if (kvMap != null && !kvMap.isEmpty()) {
        for (KV kv : kvMap.values()) {
          sb.append(kv.toString());
          sb.append(",\n");
        }
      }
      sb.append('}');
      return sb.toString();
    }
  }
