package com.tencent.dp.gdt.utils;

import java.io.File;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.white.parquet.ProtoWriter;
import org.white.parquet.utils.ParquetUtils;

import com.tencent.dp.gdt.utils.WechatTrainingDataProtos.WechatTrainingData;

/***********
 * 
 * @author kaynewu
 *
 * 2019年8月8日
 */
public class WechatWriterTests {


	
	public static void main(String[] args) throws Exception {
		ProtoWriter protoWriter = new ProtoWriter(WechatTrainingData.class,"D:\\test\\olap\\wechat.parquent" + ".SNAPPY", 
				null,CompressionCodecName.SNAPPY);

		readData("D:\\test\\olap\\olap.dat", protoWriter);
		protoWriter.close();
	
		
	}
	
	public static void readData(String path,ProtoWriter protoWriter) throws  Exception {
		File file = new File(path);
		if ( !file.exists() ) { return; }
		if(file.isFile()) {
			long pts = 0;
			long its = 0;
			long count = 0;
			RandomAccessFile f = new RandomAccessFile(path, "r");
			while(true ) {
				try {
					long ts = System.currentTimeMillis();
					byte[] a = new byte[4];
					f.readFully(a);
					int len = ParquetUtils.bytesToInt(a);
					byte[] b = new  byte[len];
					f.readFully(b);
					WechatTrainingData data = WechatTrainingData.parseFrom(b);
					pts = pts + (System.currentTimeMillis() - ts);
					ts = System.currentTimeMillis();
					protoWriter.write(data);
					count += 3;
					its = its + (System.currentTimeMillis() - ts);
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}	
			}
			System.out.println("Parser TimeUsed:" + pts + "ms Count:" + count);
			System.out.println("Index TimeUsed:" + its + "ms Count:" + count);
		}else if(file.isDirectory()){
			File[] flst = file.listFiles();
			for (int i = 0; i < flst.length; i++) {
				readData(flst[i].getAbsolutePath(), protoWriter);
			}
		}
	}
	
}
