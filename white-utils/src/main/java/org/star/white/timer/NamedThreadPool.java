package org.star.white.timer;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;


public class NamedThreadPool {


	public static class NamedThreadFactory implements ThreadFactory {
	        static final AtomicInteger poolNumber = new AtomicInteger(1);
	        final ThreadGroup group;
	        final AtomicInteger threadNumber = new AtomicInteger(1);
	        final String namePrefix;

	        NamedThreadFactory(String name) {
	            SecurityManager s = System.getSecurityManager();
	            group = (s != null)? s.getThreadGroup() :
	                                 Thread.currentThread().getThreadGroup();
	            namePrefix = "pool-"+name+"-" +
	                          poolNumber.getAndIncrement() +
	                         "-thread-";
	        }

	        public Thread newThread(Runnable r) {
	            Thread t = new Thread(group, r,
	                                  namePrefix + threadNumber.getAndIncrement(),
	                                  0);
	            if (t.isDaemon())
	                t.setDaemon(false);
	            if (t.getPriority() != Thread.NORM_PRIORITY)
	                t.setPriority(Thread.NORM_PRIORITY);
	            return t;
	        }
	    }
	
	public static class NamedLowThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        NamedLowThreadFactory(String name) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null)? s.getThreadGroup() :
                                 Thread.currentThread().getThreadGroup();
            namePrefix = "pool-"+name+"-" +
                          poolNumber.getAndIncrement() +
                         "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.MIN_PRIORITY)
                t.setPriority(Thread.MIN_PRIORITY);
            return t;
        }
    }
	
	public static class NamedHighThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        NamedHighThreadFactory(String name) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null)? s.getThreadGroup() :
                                 Thread.currentThread().getThreadGroup();
            namePrefix = "pool-"+name+"-" +
                          poolNumber.getAndIncrement() +
                         "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.MAX_PRIORITY)
                t.setPriority(Thread.MAX_PRIORITY);
            return t;
        }
    }
	
	public static abstract class RunWithName  implements  Runnable {
		public abstract String debugmsg();
		public int MaxQueue(){
			return 100000000;
		}

		public abstract void run();
	}
	
	public static class ThreadPool extends ThreadPoolExecutor
	{
	    private static Logger LOG = Logger.getLogger(ThreadPool.class);
//		ExecutorCompletionService<Object> submit ;

	    BlockingQueue<Runnable> Queue;
		public ThreadPool(int corePoolSize,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue,
                ThreadFactory threadFactory) {
			super(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                workQueue,
                 threadFactory);
			this.Queue=workQueue;
//			this.submit= new ExecutorCompletionService<Object>(this);
		}
		
		@Override
	    public void execute(Runnable command) {
			if(command instanceof RunWithName)
			{
				this.executeWithname((RunWithName)command);
				return;
			}
			final long ts=System.currentTimeMillis();
			final Runnable commandf =command;
			super.execute(new Runnable() {
				
				@Override
				public void run() {
					long diff=System.currentTimeMillis()-ts;
					if(diff>10000l)
					{
						LOG.info("ThreadPoolBusy:"+commandf.getClass().getName()+",diff:"+diff);
					}
					commandf.run();
				}
			});
	    	
		}
		
	    private void executeWithname(final RunWithName command) {
			final long ts=System.currentTimeMillis();
			
			if(this.Queue.size()>command.MaxQueue())
			{
				LOG.info("ThreadPoolBusy Drop :" + command.getClass().getName()+","+command.debugmsg()+",qsize:"+this.Queue.size());
				return ;
			}
			super.execute(new Runnable() {
				
				@Override
				public void run() {
					long diff=System.currentTimeMillis()-ts;
					if(diff>10000l)
					{
						LOG.info("ThreadPoolBusy:"+command.getClass().getName()+","+command.debugmsg()+",diff:"+diff);
					}
					try{
						command.run();
					}catch(Throwable e)
					{
						LOG.error("ThreadPoolERROR:"+command.getClass().getName()+","+command.debugmsg()+",diff:"+diff,e);
					}
				}
			});
	    	
		}
		
	}
	
	public static ThreadPoolExecutor create(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,String s)
	{
		return new ThreadPool(Math.max(corePoolSize, 1), maximumPoolSize, keepAliveTime, unit, workQueue,new NamedThreadFactory(s));
	}
	
	public static ThreadPoolExecutor createHigh(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,String s)
	{
	
		return new ThreadPool(Math.max(corePoolSize, 1), maximumPoolSize, keepAliveTime, unit, workQueue,new NamedHighThreadFactory(s));
	}
	
	public static ThreadPoolExecutor createLow(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,String s)
	{
	
		return new ThreadPool(Math.max(corePoolSize, 1), maximumPoolSize, keepAliveTime, unit, workQueue,new NamedLowThreadFactory(s));
	}
}
