package com.alibaba.middleware.race.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.middleware.race.config.RaceConfig;
import com.alibaba.middleware.race.config.RaceConfig.QueryFileType;
import com.alibaba.middleware.race.index.model.StoreInfo;

/*
 * 一个专门用于读文件的IO线程
 */
public class IOThreadV3 implements Runnable {
	
	private ConcurrentLinkedQueue<TaskV3> ioTaskQueue = new ConcurrentLinkedQueue<TaskV3>();
	
	public IOThreadV3() 
	{
		
	}
	
	public void submitTask(TaskV3 task)
	{
		this.ioTaskQueue.add(task);
	}
	
	@Override
	public void run() 
	{
		while(true)
		{
			while(!ioTaskQueue.isEmpty())
			{
				TaskV3 task = ioTaskQueue.poll();
				byte[] result = null;
				try {
					result = dealIO(task.getOffset(), task.getLength(), task.getRaf());
				} catch (Exception e) {
					e.printStackTrace();
				}
				//byte[] result = dealIO(task.getOffset(), task.getLength(), task.getRaf());
				task.onIOFinish(result);
				
			}
		}
	}
	
	private byte[] dealIO(long offset,int length,RandomAccessFile raf)
	{
		byte[] bbuf = null;
		if(raf == null){
			System.out.println("raf文件为null");
		}
		try{
			bbuf = new byte[length];
			raf.seek(0);
			raf.seek(offset);
			raf.read(bbuf);
		}catch(Exception e){
			e.printStackTrace();
		}
		return bbuf;
	}
	

}
