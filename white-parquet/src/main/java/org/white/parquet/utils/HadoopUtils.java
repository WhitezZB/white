package org.white.parquet.utils;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {
	
	public static Configuration grabConfiguration(String hadoopConfDir, Configuration conf) {
		boolean oldVersionHadoop = new File(hadoopConfDir, "hadoop-default.xml").exists() || new File(hadoopConfDir, "hadoop-site.xml").exists(); 
		String[] files = null;
		if(oldVersionHadoop){
			files = new String[]{"hadoop-default.xml","hadoop-site.xml","yarn-site.xml"};
		}else{
			files = new String[]{"hdfs-site.xml","httpfs-site.xml","mapred-site.xml",
					"core-site.xml","yarn-site.xml","fair-scheduler.xml","hadoop-policy.xml","capacity-scheduler.xml"};
			
		}
		for(String xml : files){
			try {
				conf.addResource(new Path(hadoopConfDir, xml));
			} catch (Exception e) {
				// TODO: handle exception
			}				
		}
		return conf;
	}
	
}
