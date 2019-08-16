package com.tencent.sdk.index.directory;

import org.apache.log4j.Logger;

public class DirectoryParams {
	
	public static Logger LOG = Logger.getLogger(DirectoryParams.class);
	
	public String tablename;
	public String partition;
	public int partNo;
	public String hdfsPath;
	
	
	public DirectoryParams(String hdfsPath, String tablename, String partition, int partNo){
		this.tablename = tablename;
		this.partition = partition;
		this.partNo = partNo;
		LOG.info("params hdfsPath:" + hdfsPath + ", tablename:" + tablename 
				+ ", partition:" + partition + ", partNo:" + partNo);
	}
}
