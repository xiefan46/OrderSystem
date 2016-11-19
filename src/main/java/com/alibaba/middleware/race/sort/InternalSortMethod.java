package com.alibaba.middleware.race.sort;

import java.util.Comparator;
import java.util.List;

public interface InternalSortMethod<T> {
	/*
	 * 传入需要排序的数组和Comparator,对传入的数组排好序
	 */
	void sort(T[] input,Comparator<T> cmp);
}
