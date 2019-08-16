package com.tencent.hermes.store.hdfs.block;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.tencent.hermes.store.hdfs.HdfsHermesInput;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

public class BlockCacheRam extends BlockCache{
	private static final Log LOG = LogFactory.getLog(BlockCacheRam.class);
	public static volatile ConcurrentMap<List<Long>, BlockValue> fileCacheGlobal = null;
	public static AtomicInteger usedCache = new AtomicInteger(0);
	public static int BLOCK_SIZE_OFFSET = 13;// 20=1024k 19=512k 18=256k 17=128k;
												// 16=1024*64 15=1024*32
												// 14=1024*16 13=1024*8
												// 12=1024*4 11=1024*2 10=1024
												// 9=512 8=256 7=128 6=64
	public static int BLOCK_SIZE = 1 << BLOCK_SIZE_OFFSET;
 
	static {
		StartCache(1024, BLOCK_SIZE_OFFSET);
	}

	public static void StartCache(int size, int BLOCK_SIZE_OFFSET) {
	
		BlockCacheRam.BLOCK_SIZE_OFFSET = BLOCK_SIZE_OFFSET;
		BlockCacheRam.BLOCK_SIZE = 1 << BLOCK_SIZE_OFFSET;
		ConcurrentMap<List<Long>, BlockValue> fileCacheGlobalold = BlockCacheRam.fileCacheGlobal;

		BlockCacheRam.fileCacheGlobal = new ConcurrentLinkedHashMap.Builder<List<Long>, BlockValue>()
				.maximumWeightedCapacity(size).listener(new EvictionListener<List<Long>, BlockValue>() {
					@Override
					public void onEviction(List<Long> key, BlockValue block) {

					}
				})
				.build();
		if (fileCacheGlobalold != null) {
			BlockCacheRam.fileCacheGlobal.putAll(fileCacheGlobalold);
		}
		usedCache.set(size);

		LOG.info("start cache:" + size + ","
				+ BlockCacheRam.BLOCK_SIZE_OFFSET + ","
				+ BlockCacheRam.BLOCK_SIZE);
	

	}


	public BlockCacheRam(HdfsHermesInput input) {
		 super(input);
	}


	public BlockValue lastbuff = new BlockValue(0);
	public Long lastBlockIndex = -1l;

	public static AtomicLong readTimes = new AtomicLong(0);
	public static AtomicLong getbuffTimes = new AtomicLong(0);
	public static AtomicLong reuseTimes = new AtomicLong(0);
	public static AtomicLong reuseTimesCurrent = new AtomicLong(0);

	private static AtomicLong times = new AtomicLong(0);
	private static long ts = System.currentTimeMillis();
	private static final ConcurrentMap<List<Long>, Object> lockCache = new ConcurrentLinkedHashMap.Builder<List<Long>, Object>()
			.maximumWeightedCapacity(40960)
			.listener(new EvictionListener<List<Long>, Object>() {
				@Override
				public void onEviction(List<Long> key, Object location) {

				}
			}).build();

	public static Object lockCacheLock = new Object();

	public static <T, K> T UpdateGet(ConcurrentMap<K, T> cacheLru, K key) {
		T w = cacheLru.get(key);
		if (w == null) {
			return null;
		}
		cacheLru.put(key, w);
		return w;
	}

	public static <K> Object getLock(ConcurrentMap<K, Object> lockCache, K key) {
		Object w = lockCache.get(key);
		if (w == null) {
			synchronized (lockCacheLock) {
				 w = lockCache.get(key);
				if (w == null) {
					w = new Object();
					lockCache.put(key, w);
				}
			}
		}
		return w;

	}

	private int getbuff(long position, byte[] b, int offset, int len)
			throws IOException {
		getbuffTimes.incrementAndGet();
		long blockIndex = position >> BlockCacheRam.BLOCK_SIZE_OFFSET;
		long blocktart = blockIndex << BlockCacheRam.BLOCK_SIZE_OFFSET;
		BlockValue blockdata = null;
		if (blockIndex == this.lastBlockIndex) {
			blockdata = this.lastbuff;
		}

		if (blockdata == null) {
			List<Long> blk = Arrays.asList(this.input.inputStream.getSeqId(),blockIndex);//(this.input.path, this.input.name,blockIndex);
			blockdata = (BlockValue) UpdateGet(BlockCacheRam.fileCacheGlobal, blk);

			if (blockdata == null) {
				synchronized (getLock(lockCache, blk)) {
					blockdata = (BlockValue) UpdateGet(	BlockCacheRam.fileCacheGlobal, blk);

					if (blockdata == null) {
						long end = this.input.inputStream.getLength();
						int size = BlockCacheRam.BLOCK_SIZE;
						if (blocktart + size >= end) {
							size = (int) (end - blocktart);
						}
						blockdata = new BlockValue(size);
						this.input.inputStream.readFully(blocktart, blockdata.buff, 0,blockdata.size);
//						LOG.info(this.input.name+","+blocktart+","+blockdata.size);
						readTimes.incrementAndGet();					
						BlockCacheRam.fileCacheGlobal.put(blk, blockdata);
					}else{
						reuseTimes.incrementAndGet();
					}
				}
				if (times.incrementAndGet() > 1000) {
					times.set(0);
					long ts2 = System.currentTimeMillis();
					if (ts2 - ts >= 60000l) {
						ts = ts2;
						LOG.info("reuseTimes:" + reuseTimes.get()+",reuseTimesCurrent:" + reuseTimesCurrent.get()
								+ ",readTimes:" + readTimes.get()
								+ ",getbuffTimes:" + getbuffTimes.get());
					}
				}
			}
			else{
				reuseTimes.incrementAndGet();
			}

			this.lastbuff = blockdata;
			this.lastBlockIndex = blockIndex;
		}
		else{
			reuseTimesCurrent.incrementAndGet();
		}

		int blockoffset = (int) (position - blocktart);
		int leftsize = blockdata.size - blockoffset;
		int returnsize = leftsize;
		if (len < returnsize) {
			returnsize = len;
		}
		
		System.arraycopy(blockdata.buff, blockoffset, b, offset, returnsize);
		return returnsize;
	}

	public void readInternalCache(long position,byte[] b, int offset, int length)
			throws IOException {
		int off = offset;
		int len = length;
		while (len > 0) {
			int size = this.getbuff(position, b, off, len);
			position += size;
			off += size;
			len -= size;
		}
	}

}
