package org.star.white.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import org.star.white.WhiteConfigConstant;
import org.star.white.conf.GlobalInfo;
import org.star.white.hadoop.HadoopUtil;
import org.star.white.timer.PoolTask;
import org.star.white.timer.TimerPools;



/*********
 * 
 * @author KATYNE
 *
 * 2018年5月26日
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class ConfigRead {
	private static Logger LOG = Logger.getLogger(ConfigRead.class);

	private static TimerPools REFRESH_TIMER=null;

	private static PoolTask FETCH_TASK=null;
	private static Object LOCK=new Object();
	
	private static String confPath=null;

	private static String hadoopConfPath= GlobalInfo.getProperty("hadoop.conf.dir");
	
	public static void init(int workId) throws IOException
	{
		init(String.valueOf(workId));
	}
	
	private static AtomicBoolean isInit = new AtomicBoolean(false);
	private static Object isInitObject = new Object();

	public static void init() throws IOException
	{
		initByApi();
	}
	public static void initByApi() throws IOException
	{

		synchronized (isInitObject) {
			if (!isInit.get()) {
				ConfigRead.init("9999999");
				isInit.set(true);
			}

		}
	}

	public static void init(String workId) throws IOException
	{
		Properties properties = getProperties();
		Map<Object, Object>whitesconf=new HashMap<Object, Object>();
		whitesconf.put("white.hadoop.conf.path", properties.get("white.hadoop.conf.path"));
		whitesconf.put(WhiteConfigConstant.HADOOP_CONF, properties.get(WhiteConfigConstant.HADOOP_CONF));
		ConfigRead.setUpConf(whitesconf);
		UniqConfig.registerWork(workId);

	}
	
	public static void init(String workId, Properties properties) throws IOException
	{
		Map<Object, Object>hermesconf=new HashMap<Object, Object>();
		hermesconf.put("white.conf.dir", properties.get("white.hadoop.conf.path"));
		hermesconf.put(WhiteConfigConstant.HADOOP_CONF, properties.get(WhiteConfigConstant.HADOOP_CONF));
		ConfigRead.setUpConf(hermesconf);
		UniqConfig.registerWork(workId);
	}
	
	/**
	 * for test
	 * @param workId
	 * @param conf
	 * @throws IOException
	 */
	public static void initByMap(String workId,Map conf) throws IOException
	{
		ConfigRead.confPath = String.valueOf(conf.get("white.hadoop.conf.path"));
		ConfigRead.hadoopConfPath=String.valueOf(conf.get(WhiteConfigConstant.HADOOP_CONF));
		UniqConfig.setStormconf(conf);
		UniqConfig.registerWork(workId);

	}
	

	
	
	private static void setUpConf(Map conf) {
		
		
		
		ConfigRead.confPath = String.valueOf(conf.get("white.hadoop.conf.path"));
		ConfigRead.hadoopConfPath=String.valueOf(conf.get(WhiteConfigConstant.HADOOP_CONF));
		synchronized (LOCK) {
			if (REFRESH_TIMER == null) {
				REFRESH_TIMER = new TimerPools("ConfigRead");
			}

			if (FETCH_TASK == null) {
				FETCH_TASK = new PoolTask() {
					@Override
					public void execute() {
						synchronized (LOCK) {

							try {
								HadoopUtil.upadateConf();
								Properties properties = getProperties();
								LOG.info("sync conf:" + ConfigRead.confPath);
								Map map=properties;
								if(ConfigRead.confPath != null) {
									map.putAll(ConfigRead.readStormConfig());	
								}								
								UniqConfig.setStormconf(map);
							} catch (Throwable e) {
								e.printStackTrace();
							}
						}

					}

					public String logMsg() {
						return "CommonSplitParse_timers_fetcher";
					}
				};

				REFRESH_TIMER.schedule(FETCH_TASK, 1000, 10000);
				FETCH_TASK.execute();

			}
		}


	}
	
	
	
	private static Map findAndReadCommonConfig(String name){
		try {
			Configuration conf = new Configuration();
			HadoopUtil.grabConfiguration(ConfigRead.hadoopConfPath, conf);						
			Path hdfs=new Path(ConfigRead.confPath,name);
			LOG.info("hadoop.security.authentication=" + conf.get("hadoop.security.authentication"));
			FileSystem fs =  HadoopUtil.getFs(conf, hdfs);
			if (fs.exists(hdfs)) {
				FSDataInputStream stream=null;
				stream = fs.open(hdfs);
				Properties properties = new Properties();
				if(stream!=null)
				{
					properties.load(stream);
				}
				stream.close();
				Map map=properties;
				return map;
			}			
		}catch (IOException e){
			e.printStackTrace();
		}
		
		return new HashMap<String,String>();
	}

	private static Map readStormConfig() {
		Map ret=new HashMap();
		try {
			ret.putAll(findAndReadCommonConfig(WhiteConfigConstant.hadoop_properties));
					return ret;
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("hadoop_properties can't find");
		}
		return ret;

	}
	
	
	private  static Properties getProperties(){
		Properties properties = null;
		try {
			if(properties == null){
				properties = new Properties();
				LOG.info("Init GlobalInfo first");
				properties.load(ClassLoader.getSystemResourceAsStream(WhiteConfigConstant.white_properties));
				properties.load(ClassLoader.getSystemResourceAsStream(WhiteConfigConstant.user_properties));
				LOG.info("Init GlobalInfo second");
			}
		} catch(Throwable e) {	
			boolean fixedCond=false;
			try {
				LOG.info("Init GlobalInfo start jar conf");
				properties.load(ClassLoader.getSystemResourceAsStream("conf/"+WhiteConfigConstant.white_properties));
				properties.load(ClassLoader.getSystemResourceAsStream("conf/"+WhiteConfigConstant.user_properties));

				LOG.info("Init GlobalInfo end jar conf");
				fixedCond=true;
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
			}
			LOG.info("Init GlobalInfo after second exception");
			if(fixedCond){
				return properties;
			}
			try {
				LOG.info("Init GlobalInfo start");
				InputStream is1 = new FileInputStream(new File(new File("conf"), WhiteConfigConstant.white_properties));
				InputStream is2 = new FileInputStream(new File(new File("conf"), WhiteConfigConstant.user_properties));
				LOG.info("Init GlobalInfo mid");
				properties.load(is1);
				properties.load(is2);
				LOG.info("Init GlobalInfo end");
				is1.close();
				is2.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		return properties;
	}
	
}
