package org.star.white.conf;



import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.star.white.WhiteConfigConstant;

/***********
 * 
 * @author KATYNE
 *
 * 2018年5月26日
 */
public class GlobalInfo {
	public static final Logger logger = Logger.getLogger(GlobalInfo.class);
	

	static Properties  properties = null;
	static {
		init();
	}
	
	public synchronized static void init(){
		try {
			if(properties == null){
				properties = new Properties();
				logger.info("Init GlobalInfo first");
				properties.load(ClassLoader.getSystemResourceAsStream(WhiteConfigConstant.white_properties));
				properties.load(ClassLoader.getSystemResourceAsStream(WhiteConfigConstant.user_properties));
				logger.info("Init GlobalInfo second");
			}
		} catch(Throwable e) {	
			boolean fixedCond=false;
			try {
				logger.info("Init GlobalInfo start jar conf");
				properties.load(ClassLoader.getSystemResourceAsStream("conf/"+WhiteConfigConstant.white_properties));
				properties.load(ClassLoader.getSystemResourceAsStream("conf/"+WhiteConfigConstant.user_properties));

				logger.info("Init GlobalInfo end jar conf");
				fixedCond=true;
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
			}
			logger.info("Init GlobalInfo after second exception");
			if(fixedCond){
				return ;
			}
			try {
				logger.info("Init GlobalInfo start");
				InputStream is1 = new FileInputStream(new File(new File("conf"), WhiteConfigConstant.white_properties));
				InputStream is2 = new FileInputStream(new File(new File("conf"), WhiteConfigConstant.user_properties));
				logger.info("Init GlobalInfo mid");
				properties.load(is1);
				properties.load(is2);
				logger.info("Init GlobalInfo end");
				is1.close();
				is2.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
	
	public static final GlobalInfo DEFAULT = new GlobalInfo();

	public GlobalInfo() {
	}
	

	

	
	/**
	 * 
	 * @return
	 */
	public final static Properties getProperties() {
		if(properties == null){
			init();
		}
		return properties;
	}
	
	/**
	 * 
	 * @param filenameInClasspath
	 * @return
	 */
	public final static Properties getProperties(String filenameInClasspath) {
		Properties properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream(filenameInClasspath));
		} catch(IOException e) {
			logger.error(e);
		}
		return properties;
	}





	
	public final static String getProperty(String key) {
		if(properties == null){
			init();
		}
		return properties.getProperty(key);
	}
	
	public final static String getProperty(String key, String defaultValue) {
		if(properties == null){
			init();
		}
		return properties.getProperty(key, defaultValue);
	}
	
	public final static Boolean getBoolean(String key, Boolean defaultValue) {
		if(properties == null){
			init();
		}
		return Boolean.parseBoolean(properties.getProperty(key, defaultValue + ""));
	}
	
	public final static int getInt(String key, Integer defaultValue) {
		if(properties == null){
			init();
		}
		return Integer.parseInt(properties.getProperty(key, defaultValue + ""));
	}
	
	public final static long getLong(String key, Long defaultValue) {
		if(properties == null){
			init();
		}
		return Long.parseLong(properties.getProperty(key, defaultValue + ""));
	}
	
	public final static float getFloat(String key, Float defaultValue) {
		if(properties == null){
			init();
		}
		return Float.parseFloat(properties.getProperty(key, defaultValue + ""));
	}

	public final static double getDouble(String key, Double defaultValue) {
		if(properties == null){
			init();
		}
		return Double.parseDouble(properties.getProperty(key, defaultValue + ""));
	}
	
	public static String getCurrentPath() {
		return new File("").getAbsolutePath();
	}
}
