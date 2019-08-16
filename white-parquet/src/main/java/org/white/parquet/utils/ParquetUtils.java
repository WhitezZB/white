package org.white.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;

/**********
 * 
 * @author kaynewu
 *
 * 2019年7月22日
 */
public class ParquetUtils {
	
	public static void addGroup(Group group,String key,Object value) {
		if(value instanceof String) {
			group.append(key, (String)value);
			return;
		}
		if(value instanceof Integer) {
			group.append(key, (Integer)value);
			return;
		}
		if(value instanceof Long) {
			group.append(key, (Long)value);
			return;
		}
		if(value instanceof Double) {
			group.append(key, (Double)value);
			return;
		}
		if(value instanceof Float) {
			group.append(key, (Float)value);
			return;
		}
		if(value instanceof Boolean) {
			group.append(key, (Boolean)value);
			return;
		}
		if(value instanceof Binary) {
			group.append(key, (Binary)value);
			return;
		}
		if(value instanceof NanoTime) {
			group.append(key, (NanoTime)value);
			return;
		}
	}
	
	
	public static String formatPartFile(String filePath) {
		return filePath + "." + System.nanoTime();
	}
	
	public static int bytesToInt(byte[] src) {
		 int offset = 0;
		  int value; 
		  value = (int) ((src[offset] & 0xFF) 
		    | ((src[offset+1] & 0xFF)<<8) 
		    | ((src[offset+2] & 0xFF)<<16) 
		    | ((src[offset+3] & 0xFF)<<24));
		  return value;
	}
	
	private static Configuration remoteConf;
	private static Object confLock = new Object();
	public static Configuration getDefaultConf(){
		synchronized (confLock) {
			if(remoteConf == null){
				remoteConf = new Configuration();
			}
			return remoteConf;
		}
	}

}
