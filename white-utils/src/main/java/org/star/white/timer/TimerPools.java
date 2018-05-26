package org.star.white.timer;


import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TimerPools {
	 private static ThreadPoolExecutor EXECUTE=null;
	 private static Object EXELOCK=new Object();
	 private Timer timer;
	 

	public TimerPools(String name)
	{
		timer=new Timer(name);
		synchronized (EXELOCK) {
			if(EXECUTE==null)
			{
				EXECUTE=NamedThreadPool.createHigh(3, Integer.MAX_VALUE, 600L, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>(),name+"_tpool");
			}
		}
	}
	
    public void schedule(PoolTask task, long delay, long period) {
    	synchronized (EXELOCK) {
    		task.setEXECUTE(EXECUTE);  
    	}
    	timer.schedule(task, delay+(long)(Math.random()*delay/10),period+(long)(Math.random()*period/10));
    }
    
    
    public void schedule(PoolTask task, long delay) {
    	synchronized (EXELOCK) {
    		task.setEXECUTE(EXECUTE);  	
    	}
    	timer.schedule(task, delay+(long)(Math.random()*delay/10));
    }
    
    public int purge() {
    	return timer.purge();
    }


}
