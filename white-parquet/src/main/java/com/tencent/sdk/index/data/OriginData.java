package com.tencent.sdk.index.data;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;


/*****
 * 
 * @author kaynewu
 *
 * 2019年7月30日
 */
public abstract class OriginData<T> implements Serializable{

	private static final long serialVersionUID = 6200319973812244722L;
	
	String tablename;
	private T data;
	private long seqId;
	
	public OriginData(String tablename, T data) {
		this.tablename = tablename;
		this.data = data;
		this.seqId = -1;
	}
	
	public T getData() {
		return data;
	}
	
	public byte[] toByte() {
		// TODO Auto-generated method stub
		try {
			ByteArrayOutputStream baos= new ByteArrayOutputStream();
			ObjectOutputStream oos=new ObjectOutputStream(baos);
			oos.writeObject(this);  
		    oos.close();
		    baos.close();
		    byte[] data = baos.toByteArray();
		    return data;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}

	public long getSeqId() {
		return seqId;
	}

}
