package org.white.parquet.index;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/****
 * 
 * @author kaynewu
 *
 * 2019年7月25日
 */
public class ThreadPools {
	public static final ExecutorService indexPool = Executors.newCachedThreadPool();
	
	
	public static void submit(Runnable runable) {
		indexPool.execute(runable);
	}
}
