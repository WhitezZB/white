package com.tencent.sdk.index.directory;

import com.tencent.sdk.index.data.IndexDocument;

/*************
 * 数据分片目录抽象类
 * @author kaynewu
 *
 * 2019年7月31日
 */
public abstract class AbstractDirectory<T> {
	// 目录属性相关参数
	protected DirectoryParams params;
	// 时间相关参数
	protected DirectoryTimes times = new DirectoryTimes();
	//
	protected DirectoryLocks locks = new DirectoryLocks();
	
	public AbstractDirectory(String hdfsPath, String tablename, String partition, int partNo) {
		this.params = new DirectoryParams(hdfsPath, tablename, partition, partNo);
	}
	
	// 添加索引记录
	public abstract void addDoc(IndexDocument<T> doc);
	
	// 数据刷入磁盘
	public abstract void flushDisk();
	
	// 合并数据文件减少原数据压力
	public abstract void mergeFile();
	
	
	// 当前读写索引块是否超过规定大小
	public abstract boolean overSize(); 
	
	// 设置为只读目录
	public abstract boolean setForRead();
	
	// 设置为只写目录
	public abstract boolean setForWrite();
	
	
	// 是否需要进行刷盘
	public boolean needFlush() {
		if(this.overSize() || this.isFlushTimeout()) {
			return true;
		}
		return false;
	}
	
	// 是否达到刷盘时间上限
	public abstract boolean isFlushTimeout();
	
	
}
