package org.star.white.timer;


import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;






public class TimeCacheMap<K, V> implements Serializable{
	private static final long serialVersionUID = 1L;
    private static final int DEFAULT_NUM_BUCKETS = 3;
	private static Logger LOG = Logger.getLogger(TimeCacheMap.class);

    public static interface ExpiredCallback<K, V> {
        public void expire(K key, V val);
        public void commit();
    }

    private LinkedList<Map<K, V>> _bucketsa;

    private final Object _lock = new Object();
    private Thread _cleaner=null;
    PoolTask task=null;
    TimerPools timer=null;
    
    
    private ExpiredCallback<K, V> _callback;
    
    
    private int numBuckets;
	private long lasttime=System.currentTimeMillis();
	private long localMergerDelay=20*1000l;
	
	public static class CleanExecute<K, V>
 {
		public void executeClean(TimeCacheMap<K, V> t) {
			t.clean();
		}
	}
	
	public static class CleanExecuteWithLock<K, V> extends CleanExecute<K, V>
	{
		Object lock;
	    public CleanExecuteWithLock(Object lock) {
			this.lock = lock;
		}
		public void executeClean(TimeCacheMap<K, V> t){
			
			synchronized (this.lock) {
				super.executeClean(t);

			}
	    }
		
	}
	

	
	CleanExecute<K, V> _cleanlock=new CleanExecute<K, V>();

	  private final static float LOADFACTOR = 0.75f;

	public Map<K, V> createMap(final int sz)
	{
		if(sz>102400000)
		{
			return new HashMap<K, V>(); 
		}
		return new LinkedHashMap<K,V>((int) Math.ceil(sz / LOADFACTOR) + 1, LOADFACTOR, true) {
		      @Override
		      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {

		    	  boolean rtn=size() > sz;
		    	  if(rtn)
		    	  {
		    		  if(_callback!=null)
		    		  {
		               	synchronized(_callback) {
							LOG.info("expire "+eldest.getKey());

		    			  _callback.expire(eldest.getKey(), eldest.getValue());
			              _callback.commit(); 
		               	}
		    		  }
	                   
	             			    	  
		    	  }
		        return rtn;
		      
		      }
		    };
	}
    
	int linkLen=Integer.MAX_VALUE;
    public TimeCacheMap(int linkLen,TimerPools timer,int expirationSecs, int numBuckets, ExpiredCallback<K, V> callback,CleanExecute<K, V> cleanlock) {
    	this.linkLen=linkLen;
    	if(cleanlock!=null)
    	{
    		this._cleanlock=cleanlock;
    	}
    	
    	this.numBuckets=numBuckets;
        if(numBuckets<2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _bucketsa = new LinkedList<Map<K, V>>();
        for(int i=0; i<numBuckets; i++) {
            _bucketsa.add(createMap(this.linkLen));
        }

        _callback = callback;
        
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets-1);
        this.localMergerDelay=sleepTime-100;
        if(timer==null)
        {
        _cleaner = new Thread(new Runnable() {
            public void run() {
					while (true) {
						LOG.info("_cleaner start");
						try {
							Thread.currentThread().sleep(sleepTime);
							TimeCacheMap.this.maybeClean();
						} catch (Throwable ex) {
							LOG.error("_cleaner", ex);
						}
					}

            }
        });
        _cleaner.setDaemon(true);
        _cleaner.start();
        
        }else{
        	task=new PoolTask() {
				@Override
				public void execute() {
					try {
						TimeCacheMap.this.maybeClean();
					} catch (Throwable ex) {
						LOG.error("_cleaner", ex);
					}
				}
			};
        	timer.schedule(task, sleepTime,sleepTime);
        	this.timer=timer;

        }
    }
    
    
     
    private  void timerReset()
	{
		lasttime=System.currentTimeMillis();
	}
    
	private boolean isTimeout()
	{
		long time=System.currentTimeMillis();
    	if((lasttime+localMergerDelay)<=time)
		{
			return true ;
		}
    	
    	return false;
	}
    
    public void maybeClean()
    {
        synchronized(_lock) {
	    	if(!this.isTimeout())
	    	{
	    		return ;
	    	}
	    	this.timerReset();
        }
		this._cleanlock.executeClean(this);
    }
    
    private void clean()
    {
    	try {
            Map<K, V> dead = null;
            synchronized(_lock) {
                dead = _bucketsa.removeLast();
                _bucketsa.addFirst(createMap(this.linkLen));
            
            }
            if(_callback!=null) {
             	synchronized(_callback) {
	                for(Entry<K, V> entry: dead.entrySet()) {
	                    _callback.expire(entry.getKey(), entry.getValue());
	                }
	                _callback.commit();
             	}
            }
            
           
        
        } catch (Throwable ex) {
				LOG.error("_cleaner maybeClean", ex);
        }	
    }
    
    
    public void fourceClean()
    {
        synchronized(_lock) {
	    	this.timerReset();
        }
        this.clean();					
	
    }
    
    public void fourceTimeout(Timeout<K, V> fetch,Update<K, V> d)
    {
    	LinkedList<Map<K, V>> lastdata=null;
    	synchronized(_lock) {
    		lastdata=_bucketsa;
    		_bucketsa = new LinkedList<Map<K, V>>();
    		 for(int i=0; i<numBuckets; i++) {
    	            _bucketsa.add(createMap(this.linkLen));
    	     }
    	}

         if(_callback!=null&&lastdata!=null) {
          	synchronized(_callback) {

	        	 HashMap<K, V> needupdate=new HashMap<K, V>();
	        	 for(Map<K, V> dead:lastdata)
	        	 {
		             for(Entry<K, V> entry: dead.entrySet()) {
		            	 K key=entry.getKey();
		            	 V val=entry.getValue();
		            	 if(fetch.timeout(key,val))
		            	 {
		            		 _callback.expire(key, val);
		            	 }else{
		            		 needupdate.put(key, val);
		            	 }
		             }
	        	 }
	        	 
	        	 _callback.commit();
	        	 
	
	        	 if(needupdate.size()>0)
	        	 {
	        		 this.updateAll(needupdate, d);
	        	 }
          	}
        	 
         }
    }
    
    public void fourceTimeout()
    {
    	LinkedList<Map<K, V>> lastdata=null;
    	synchronized(_lock) {
    		lastdata=_bucketsa;
    		_bucketsa = new LinkedList<Map<K, V>>();
    		 for(int i=0; i<numBuckets; i++) {
    	            _bucketsa.add(createMap(this.linkLen));
    	        }
    	}

         if(_callback!=null&&lastdata!=null) {
         	synchronized(_callback) {

        	 for(Map<K, V> dead:lastdata)
        	 {
	             for(Entry<K, V> entry: dead.entrySet()) {
	                 _callback.expire(entry.getKey(), entry.getValue());
	             }
        	 }
        	 
        	 _callback.commit();
         	}
         }
    }
    
    
    public void clear()
    {
    	synchronized(_lock) {
    		_bucketsa = new LinkedList<Map<K, V>>();
    		 for(int i=0; i<numBuckets; i++) {
    	            _bucketsa.add(createMap(this.linkLen));
    	        }
    	}
    }

//    public TimeCacheMap(int expirationSecs, ExpiredCallback<K, V> callback) {
//        this(null,expirationSecs, DEFAULT_NUM_BUCKETS, callback,null);
//    }
    
    public TimeCacheMap( TimerPools timer,int expirationSecs, ExpiredCallback<K, V> callback) {
        this(Integer.MAX_VALUE,timer,expirationSecs, DEFAULT_NUM_BUCKETS, callback,null);
    }
    
    public TimeCacheMap( TimerPools timer,int expirationSecs, ExpiredCallback<K, V> callback,CleanExecute<K, V> cleanlock) {
        this(Integer.MAX_VALUE,timer,expirationSecs, DEFAULT_NUM_BUCKETS, callback,cleanlock);
    }
 
    public TimeCacheMap( int maxlen,TimerPools timer,int expirationSecs, ExpiredCallback<K, V> callback,CleanExecute<K, V> cleanlock) {
        this(maxlen,timer,expirationSecs, DEFAULT_NUM_BUCKETS, callback,cleanlock);
    }

//    public TimeCacheMap(int expirationSecs) {
//        this(expirationSecs, DEFAULT_NUM_BUCKETS);
//    }
//
//    private TimeCacheMap(int expirationSecs, int numBuckets) {
//        this(null,expirationSecs, numBuckets, null,null);
//    }


    public boolean containsKey(K key) {
        synchronized(_lock) {
            for(Map<K, V> bucket: _bucketsa) {
                if(bucket.containsKey(key)) {
                    return true;
                }
            }
            return false;
        }
    }
    
    public static interface eachCall<K, V>{
    	public void each(K k,V v);
    }
    public void each(eachCall<K, V> ea) {
        synchronized(_lock) {
            for(Map<K, V> bucket: _bucketsa) {
                for(Entry<K, V> e: bucket.entrySet())
                {
                	ea.each(e.getKey(), e.getValue());
                }
            }
        }
    }
    
    
  
    
    public V get(K key) {
        synchronized(_lock) {
            return this.getNolock(key);
        }
    }
    
    public V getUpdate(K key) {
        synchronized(_lock) {
	       V rtn=this.getNolock(key);
	       if(rtn!=null)
	       {
	    	   this.putNolock(key, rtn);
	       }
	       return rtn;
        }
    }
    
    
    private V getNolock(K key)
    {
    	 for(Map<K, V> bucket: _bucketsa) {
             if(bucket.containsKey(key)) {
                 return bucket.get(key);
             }
         }
         return null;
    }
    
    private void putNolock(K key, V value) {
            Iterator<Map<K, V>> it = _bucketsa.iterator();
            Map<K, V> bucket = it.next();
            bucket.put(key, value);
            while(it.hasNext()) {
                bucket = it.next();
                bucket.remove(key);
            }
    }

    public void put(K key, V value) {
        synchronized(_lock) {
            this.putNolock(key, value);
        }
    }
    
    public static interface Update<K, V>{
    	public V update(K key,V old,V newval);
    }
    
    public static interface Timeout<K, V>{
    	public boolean timeout(K key,V val);
    }
    
    public void updateAll(Map<K, V> bucket,Update<K, V> d)
    {
    	synchronized(_lock) {
    		for(Entry<K, V> e:bucket.entrySet())
    		{
    			K key=e.getKey();
    			V old=this.getNolock(key);
    			V newval=e.getValue();
    			V finalVal=d.update(key, old, newval);
    			this.putNolock(key, finalVal);
    		}
    	}
    }
    
    public void update(K key, V newval,Update<K, V> d)
    {
    	synchronized(_lock) {
    		V old=this.getNolock(key);
			V finalVal=d.update(key, old, newval);
			this.putNolock(key, finalVal);
    	}
    }
    
    
    public V remove(K key) {
        synchronized(_lock) {
            for(Map<K, V> bucket: _bucketsa) {
                if(bucket.containsKey(key)) {
                    return bucket.remove(key);
                }
            }
            return null;
        }
    }

    public int size() {
        synchronized(_lock) {
            int size = 0;
            for(Map<K, V> bucket: _bucketsa) {
                size+=bucket.size();
            }
            return size;
        }
    }
    @Override
    protected void finalize() throws Throwable {
        try {
        	if(_cleaner!=null)
        	{
        		_cleaner.interrupt();
        	}
        	
        	if(this.timer!=null)
        	{
        		this.task.cancel();
        		this.timer.purge();
        	}
        } finally {
            super.finalize();
        }
    }
    
    public Set<K> keySet(){
    	synchronized(_lock) {
    		Set<K> set = new HashSet<K>(this.size() + 2); 
            for(Map<K, V> bucket: _bucketsa) {
            	 Set<K> s = bucket.keySet();
            	 set.addAll(s);
            }
            return set;
        }
    }
    
}

