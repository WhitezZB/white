package com.tencent.sdk.index.data;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;


/*****
 * 
 * @author kaynewu
 *
 * 2019年7月30日
 */
@SuppressWarnings("hiding")
public abstract class DataParser<T,OriginData> extends Thread{
	
	public java.text.DateFormat yyyymmddFormat = new SimpleDateFormat("yyyyMMdd");
	
	private static final Logger logger = Logger.getLogger(DataParser.class);
	
	protected transient LinkedBlockingQueue<OriginData> dataQueue = new LinkedBlockingQueue<OriginData>(128);

	private boolean run = true;
	
	public void push(OriginData org) {
		try {
			this.dataQueue.put(org);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("push data error",e);
		}
	}
	
	private IndexDocument<T> fetch() {
		try {
			OriginData org = dataQueue.poll(5,TimeUnit.SECONDS);
			if(org != null) {
				return parser(org);
			}
		} catch (InterruptedException e) {
			logger.error("pull data error",e);
		}
		return null;
	}
	
	@Override
	public void run() {
		while(this.run || this.dataQueue.size() > 0) {
			IndexDocument<T> doc = fetch();
			if(doc != null) {
				this.sendDoc(doc);
			}
		}
	}
	
	
	public abstract IndexDocument<T> parser(OriginData org);
	
	public abstract int getPartNo();
	
	public abstract void sendDoc(IndexDocument<T> doc);
	

	public String getThedate() {
		Date date = new Date();
		return this.yyyymmddFormat.format(date);
	}
	
	public void close() {
		this.run = false;
	}
}
