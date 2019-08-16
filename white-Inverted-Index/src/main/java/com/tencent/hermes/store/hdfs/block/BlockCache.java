package com.tencent.hermes.store.hdfs.block;

import java.io.IOException;


import com.tencent.hermes.store.hdfs.HdfsHermesInput;

public abstract class BlockCache {
	
	
	protected final HdfsHermesInput input;
	public BlockCache(HdfsHermesInput input){
		this.input = input;
	}

	public abstract void readInternalCache(long position,byte[] b, int offset, int length)
			throws IOException;
}
