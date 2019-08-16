package com.tencent.sdk.index.data;


/*****
 * 
 * @author kaynewu
 *
 * 2019年7月30日
 */
public  class IndexDocument<T> {
	public String tablename;
	public String thedate;
	public int partNo;
	public long seqId;
	public T indexObject;
	
	public IndexDocument(String tablename,String thedate,int partNo, T indexObject, long seqId) {
		this.tablename = tablename;
		this.thedate = thedate;
		this.partNo = partNo;
		this.indexObject = indexObject;
		this.seqId = seqId;
	}
}
