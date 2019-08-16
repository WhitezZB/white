package org.white.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**********
 * 
 * @author kaynewu
 *
 * 2019年7月22日
 */
public abstract class PWriter{
	
	 int docCount;
	 String path;
	 Configuration conf;
	
	abstract public void close();
	
	abstract public void write(Object t) throws IOException;
	
	final public int getDocCount() {
		return this.docCount;
	}
	
	final public String getPath() {
		return this.path;
	}
	
	final public Configuration getConf() {
		return this.conf;
	}
}
