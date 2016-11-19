package com.alibaba.middleware.race.io;


import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.alibaba.middleware.race.config.RaceConfig.QueryFileType;
import com.alibaba.middleware.race.index.model.StoreInfo;

public class TaskV3 implements IOCallback{
	
	private long offset;
	
	private int length;
	
	private RandomAccessFile raf;
	
	private final BlockingQueue<byte[]> answer = new LinkedBlockingDeque<byte[]>();
	
	
	public TaskV3(long offset,int length,RandomAccessFile raf)
	{
		this.offset = offset;
		this.length = length;
		this.raf = raf;
	}
	
	public byte[] getIOResult()
	{
		boolean interrupt = false;
		byte[] data = null;
		while(true)
		{
			try{
				data = answer.take();
				break;
			}catch(InterruptedException e){
				interrupt = true;
				e.printStackTrace();
			}
		}
		if(interrupt){
			Thread.currentThread().interrupt();
		}
		return data;
	}
	
	@Override
	public void onIOFinish(byte[] data)
	{
		answer.offer(data);
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public RandomAccessFile getRaf() {
		return raf;
	}

	public void setRaf(RandomAccessFile raf) {
		this.raf = raf;
	}
	
	
	
}
