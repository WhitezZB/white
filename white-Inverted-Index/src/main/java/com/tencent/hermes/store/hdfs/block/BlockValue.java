package com.tencent.hermes.store.hdfs.block;


public class BlockValue{
	public byte[] buff=null;
	public int size;
	
	@Override
	public String toString() {
		return "blockData [buff=" + (buff==null?0:buff.length)+ ", size="
				+ size  + ", lasttime="
				+ lasttime + "]";
	}
	
	
	
	public BlockValue(int size) {
		if(size==0)
		{
			this.buff = null;
	
		}else{
			this.buff =new byte[size];

		}
		this.size=size;
	}
	
	long lasttime=System.currentTimeMillis();
	public long getLasttime() {
		return lasttime;
	}

	public void updateLasttime() {
		this.lasttime = System.currentTimeMillis();
	}

	
	public long memSize() {
		if(buff==null)
		{
			return Integer.SIZE/8;
		}
		return buff.length+Integer.SIZE/8;
	}

	
}
