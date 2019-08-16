package com.tencent.hermes.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.tencent.hermes.store.HermesInput;
import com.tencent.hermes.store.hdfs.block.Descriptor;
import com.tencent.hermes.store.hdfs.block.BlockCacheRam;;


public class HdfsHermesInput extends CustomBufferedIndexInput{
	
	public final Path path;
    public final Descriptor inputStream;
    private final long length;
    private boolean clone = false;
    private BlockCacheRam blockCache;
   

    public HdfsHermesInput(Configuration conf, Path path, int bufferSize) throws IOException {
		super("HdfsHermesInput=" + path,bufferSize);
		this.path = path;
		FileSystem fs = path.getFileSystem(conf);
		FileStatus fileStatus = fs.getFileStatus(path);
	    length = fileStatus.getLen();
	    inputStream = new Descriptor(fs, path, bufferSize,length);
	    this.blockCache = new BlockCacheRam(this);
	}

	@Override
	public void closeInternal() throws IOException {
		if (!clone) {
	        inputStream.close();
	    }
	}

	@Override
	public void readInternal(byte[] b, int offset, int length) throws IOException {
		this.blockCache.readInternalCache(getFilePointer(), b, offset, length);
	}

	@Override
	public void seekInternal(long pos) throws IOException {		
	}

	@Override
	public long length() {
		return length;
	}


	@Override
	public HermesInput clone() {
		 HdfsHermesInput clone = (HdfsHermesInput) super.clone();
	     clone.clone = true;
	     clone.blockCache=new BlockCacheRam(clone);
	     return clone;
	}
}
