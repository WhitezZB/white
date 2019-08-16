package org.white.parquet.index;

import java.io.EOFException;
import java.io.RandomAccessFile;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.white.parquet.PWriter;
import org.white.parquet.ProtoWriter;
import org.white.parquet.utils.ParquetUtils;

import com.tencent.dp.gdt.utils.WechatTrainingDataProtos;
import com.tencent.dp.gdt.utils.WechatTrainingDataProtos.WechatTrainingData;

public class IndexRunner implements Runnable {
	
	private static final int MAX_DOC_COUNT = 500000;

	private static Logger logger = Logger.getLogger(IndexRunner.class);
	
	PWriter writer;
	public LinkedBlockingQueue<String> dataFileQueue = new LinkedBlockingQueue<String>();
	
	private boolean run = true;
	
	private long totalCount = 0;
	private AtomicInteger count = new AtomicInteger(0);
	
	public IndexRunner(PWriter writer) {
		this.writer = writer;
		ScheduledExecutorService printSrv = Executors.newScheduledThreadPool(1);
		printSrv.scheduleWithFixedDelay(new Runnable() {	
			@Override
			public void run() {
				try {
					float timespeed = count.get()/60;
					timespeed = Math.round(timespeed * 100) / 100f;
					logger.info( writer.getPath() + " " + "TotalCount:" + totalCount + " speed:" + timespeed + "/s");
					count.set(0);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}		
		}, 60, 60, TimeUnit.SECONDS);
	}
	
	public void addSource(String filePath) {
		this.dataFileQueue.add(filePath);
	}
	
	public void stop() {
		this.run  = false;
	}

	@Override
	public void run() {
		while(this.run || this.dataFileQueue.size() > 0) {
			try {
				String filePath = this.dataFileQueue.poll(5, TimeUnit.SECONDS);
				this.parserData(filePath);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.writer.close();
	}
	
	
	private void parserData(String filePath) throws Exception {
		RandomAccessFile f = new RandomAccessFile(filePath, "r");
		while(true ) {
			try {
				byte[] a = new byte[4];
				f.readFully(a);
				int len = ParquetUtils.bytesToInt(a);
				byte[] b = new  byte[len];
				f.readFully(b);
				WechatTrainingData data = WechatTrainingDataProtos.WechatTrainingData.parseFrom(b);
				this.writer.write(data);
				count.incrementAndGet();
				if(this.writer.getDocCount() >= MAX_DOC_COUNT) {
					this.writer.close();
					this.writer = new ProtoWriter(WechatTrainingData.class, this.writer.getPath(), 
							this.writer.getConf(),CompressionCodecName.UNCOMPRESSED);
				}
				this.totalCount ++;
			} catch (Exception e) {
				if(e instanceof EOFException) {
					break;
				}else {
					e.printStackTrace();
				}	
				break;
			}	
		}
	}

}
