package com.tencent.sdk.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.tencent.sdk.index.data.DataType;
import com.tencent.sdk.index.data.IndexDocument;

/*******
 * 
 * @author kaynewu
 *
 * 2019年7月31日
 */
public class IndexDataCenter<T> implements Runnable{
	private static final Logger logger = Logger.getLogger(IndexDataCenter.class);
	
	private static final ExecutorService Pool = Executors.newCachedThreadPool();
	
	private LinkedBlockingQueue<IndexDocument<T>> recordQueue  = new LinkedBlockingQueue<IndexDocument<T>>(1024);

	private DataType type;
	private long totalCount = 0;
	private long speedCount = 0;
	private long timepoint = System.currentTimeMillis();
	
	public IndexDataCenter(DataType type){
		this.type = type;
		Pool.execute(this);
		
		ScheduledExecutorService printSrv = Executors.newScheduledThreadPool(1);
		printSrv.scheduleWithFixedDelay(new Runnable() {
			String title = type.toString().toUpperCase();
			@Override
			public void run() {
				long timeused = System.currentTimeMillis() - timepoint;
				float timespeed = (speedCount * 1000f)/timeused;
				timespeed = Math.round(timespeed * 100) / 100f;
				speedCount = 0;
				timepoint = System.currentTimeMillis();
				logger.info(this.title + " IndexDataProcess Buffer:" + recordQueue.size());
				logger.info(this.title +  " totalCount:" + totalCount + " timespeed:" + timespeed + "/s");
			}
		}, 60, 60, TimeUnit.SECONDS);
	}
	
	public void push(IndexDocument<T> record){
		try {
			this.recordQueue.put(record);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public IndexDocument<T> pull(){
		try {
			return this.recordQueue.poll(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void run() {
		while (true) {
			IndexDocument<T> doc = this.pull();
			if(doc == null) {
				continue;
			}
			this.count();
		}
		
	}
	
	private void count() {
		this.totalCount ++;
		this.speedCount ++;
	}
	
}
