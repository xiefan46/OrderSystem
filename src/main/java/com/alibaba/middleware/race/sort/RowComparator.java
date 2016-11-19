package com.alibaba.middleware.race.sort;

import java.util.Comparator;
import java.util.List;

import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.model.ComparableKeys;
import com.alibaba.middleware.race.model.Row;

public class RowComparator implements Comparator<String>
{
	public RowComparator(List<String> orderingKeys)
	{
		this.orderingKeys = orderingKeys;
	}
	
	@Override
	public int compare(String s1,String s2)
	{
		Row r1 = RaceUtil.createRowFromRowStr(s1);
		Row r2 = RaceUtil.createRowFromRowStr(s2);
		ComparableKeys c1 = new ComparableKeys(this.orderingKeys, r1);
		ComparableKeys c2 = new ComparableKeys(this.orderingKeys, r2);
		return c1.compareTo(c2);
	}
	
	private List<String> orderingKeys;
	
}