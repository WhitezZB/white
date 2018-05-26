package org.star.white.timer;

import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

public abstract class PoolTask extends TimerTask
{
	public abstract void execute();
	private ThreadPoolExecutor EXECUTE;
	private AtomicBoolean isRunning=new AtomicBoolean(false);
	private AtomicLong runStartTs=new AtomicLong(System.currentTimeMillis());
    private static Logger LOG = Logger.getLogger(PoolTask.class);

	private static long TIMEOUT=1000L*700;
	public void setEXECUTE(ThreadPoolExecutor eXECUTE) {
		EXECUTE = eXECUTE;
	}
	public String logMsg(){
		return "";
	}
	@Override
	public void run() {
		EXECUTE.submit(new NamedThreadPool.RunWithName() {
			@Override
			public void run() {
				long ts=System.currentTimeMillis();
				if(isRunning.get()&&ts-runStartTs.get()<TIMEOUT)
				{
					LOG.info("PoolTaskBusy:"+PoolTask.this.getClass().getName()+",msg:"+logMsg());
					return ;
				}
				if(isRunning.get())
				{
					LOG.info("PoolTaskTimeout:"+PoolTask.this.getClass().getName()+",msg:"+logMsg());
				}
				
				runStartTs.set(ts);
				isRunning.set(true);
				try{
					execute();
				}finally{
					isRunning.set(false);
				}
			}

			@Override
			public String debugmsg() {
				return PoolTask.this.getClass().getName()+",msg:"+logMsg();
			}
		});
	}
}
