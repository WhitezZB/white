package org.star.white.conf;

import org.star.white.WhiteUtils;
import org.star.white.hadoop.HadoopUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.hadoop.conf.Configuration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/*********
 * @author KATYNE
 *
 * 2018年5月26日
 */
@SuppressWarnings("rawtypes")
public class UniqConfig {

	private static AtomicReference<String> WorkerId=new AtomicReference<String>("unInit");
	
	private static Object lock = new Object();
	public static void registerWork(String workId) {
		WorkerId.set(String.valueOf(workId));
	}
	
	public static String getRegisterWork(){
		return WorkerId.get();
	}
	

	
	public static long parseLong(String s)
	{
		try{
			return Long.parseLong(s);
		}catch(Throwable e)
		{
			return 0l;
		}
	}
	

	  
	public static Map stormconf=new HashMap();
	public static Map getStormconf() {
		synchronized (lock) {
			return stormconf;
		}
	}
	
	static Configuration confcache = null;

	public static Configuration getConfCache() {
		synchronized (lock) {
			return HadoopUtil.getRemoteConf();
		}
	}

	public static void setStormconf(Map stormconf) {
		synchronized (lock) {
			UniqConfig.stormconf = stormconf;
			UniqConfig.confcache=null;
		}
	}
	

	
	public static int getInt(String key,int def)
	{
		Object isSet=getStormconf().get(key);
		int number;
		if(isSet==null)
		{
			number=def;
		}else{
			number=Integer.parseInt(String.valueOf(isSet));
		}
		return number;
	}

	public static double getDouble(String key,double def)
	{
		Object isSet=getStormconf().get(key);
		double number;
		if(isSet==null)
		{
			number=def;
		}else{
			number=Double.parseDouble(String.valueOf(isSet));
		}
		return number;
	}
	
	public static String getString(String key,String def)
	{
		Object isSet=getStormconf().get(key);
		String number;
		if(isSet==null)
		{
			number=def;
		}else{
			number=String.valueOf(isSet);
		}
		return number;
	}
	
	public static long getLong(String key,long def)
	{
		Object isSet=getStormconf().get(key);
		long number;
		if(isSet==null)
		{
			number=def;
		}else{
			number=Long.parseLong(String.valueOf(isSet));
		}
		return number;
	}

	public static boolean getBoolean(String key, boolean def) {
		// TODO Auto-generated method stub
		Object isSet=getStormconf().get(key);
		boolean b;
		if(isSet==null)
		{
			b=def;
		}else{
			return String.valueOf(isSet).toLowerCase().contains("true");
		}
		return b;
	}
	
	
	public static String toJson() {
		// TODO Auto-generated method stub
		try {
			return  WhiteUtils.objectMapper.writeValueAsString(stormconf);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}

