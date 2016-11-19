package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.config.RaceConfig;

/*
 * 数据库中的一个数据项
 */
public class KV implements Comparable<KV>, KeyValue {
  String key;
  String rawValue;
  
  static private String booleanTrueValue = "true";
  static private String booleanFalseValue = "false";

  boolean isComparableLong = false;
  long longValue;

  public KV(String key, String rawValue) {
    this.key = key;
    this.rawValue = rawValue;
    if (key.equals("createtime") || (key.equals("orderid") && !rawValue.equals(RaceConfig.VALUE_NOT_EXIST))) {
      isComparableLong = true;
      longValue = Long.parseLong(rawValue);
    }
  }

  public String key() {
    return key;
  }

  public String valueAsString() {
    return rawValue;
  }

  public long valueAsLong() throws OrderSystem.TypeException {
    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException e) {
      throw new OrderSystem.TypeException();
    }
  }

  public double valueAsDouble() throws OrderSystem.TypeException {
    try {
      return Double.parseDouble(rawValue);
    } catch (NumberFormatException e) {
      throw new OrderSystem.TypeException();
    }
  }

  public boolean valueAsBoolean() throws OrderSystem.TypeException {
    if (this.rawValue.equals(booleanTrueValue)) {
      return true;
    }
    if (this.rawValue.equals(booleanFalseValue)) {
      return false;
    }
    throw new OrderSystem.TypeException();
  }

  public int compareTo(KV o) {
    if (!this.key().equals(o.key())) {
      throw new RuntimeException("Cannot compare from different key");
    }
    if (isComparableLong) {
      return Long.compare(this.longValue, o.longValue);
    }
    return this.rawValue.compareTo(o.rawValue);
  }

  @Override
  public String toString() {
    return "[" + this.key + "]:" + this.rawValue;
  }
}