package com.tencent.hermes.store.hdfs.block;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;



public class Descriptor {
	    private static final Logger logger = Logger.getLogger(Descriptor.class);

		private static  LRUMap RamDirector = new LRUMap(1024);

	    private String uuid=java.util.UUID.randomUUID().toString();
	    private  FSDataInputStream in;
	    private Path file;
	    
	    private FileSystem fs;
	    private int ioFileBufferSize;
	    long index=0;
	    private final long length;

	    public long getLength() {
			return length;
		}


	    
	    public static ConcurrentMap<Object, Long> UniqInputKey = new ConcurrentLinkedHashMap.Builder<Object, Long>()
	    		.maximumWeightedCapacity(102400).listener(new EvictionListener<Object, Long>() {
			@Override
			public void onEviction(Object key, Long block) {
			}
		}).build();
		public static AtomicLong G_UNIQ_INDEX=new AtomicLong(0);
		
		
		private long seqId=0;

		public long getSeqId() {
			return seqId;
		}



		public Descriptor(FileSystem fs,Path _file, int ioFileBufferSize,long length)
		    throws IOException {
	    	this.file=_file;
	    	this.fs=fs;
	    	this.ioFileBufferSize=ioFileBufferSize;
	    	this.length=length;
	    	String key= this.file.toString();
	    	Long k=UniqInputKey.get(key);
	    	if(k==null)
	    	{
	    		k=G_UNIQ_INDEX.incrementAndGet();
	    		UniqInputKey.put(key, k);
	    	}
    		logger.info("seqId,"+k+","+key);
	    	this.seqId=k;    	
	    }
	    

	    
	    private FSDataInputStream Stream() throws IOException
	    {
	    	if(this.in==null)
	    	{
	    		this.in = fs.open(file, ioFileBufferSize);
    			logger.info("fs.open "+file.getName());
	    	}    				
			return this.in;
	    }
	    
	    protected void readFully(long position,byte[] b, int offset, int length) throws IOException
	    {
	    	synchronized (lock) {
	    		this.Stream().readFully(position, b, offset, length);
	    	}
	    	if(this.in != null){
		    	if(index == 0 || index + 1 >20)
		    	{
		    		RamDirector.put(this.uuid, this);
		    		index=0;
		    	}
		    	index = index +1;
	    	}
	    }
	    
		protected void readByteBuffer(long position, ByteBuffer buf) throws IOException {
			synchronized (lock) {
				this.Stream().seek(position);
				this.Stream().read(buf);
				buf.flip();
			}
			if(this.in != null){
				if(index == 0 || index + 1 >20)
				{
					RamDirector.put(this.uuid, this);
					index=0;
				}
				index = index +1;
			}
		}

	    
	    Object lock=new Object();
	    
	    public void close() throws IOException
	    {
	    	synchronized (lock) {
	    		if(this.in!=null)
		    	{
	    			logger.info("close "+this.file.toString());
		    		this.in.close();
		    		this.in=null;
		    	}	
			}
	    	
	    }
	    
	}