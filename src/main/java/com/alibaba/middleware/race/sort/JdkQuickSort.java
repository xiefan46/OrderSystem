package com.alibaba.middleware.race.sort;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class JdkQuickSort<T> implements InternalSortMethod<T>{

	@Override
	public void sort(T[] value, Comparator<T> cmp) {
		Arrays.sort(value, cmp);
	}

}
