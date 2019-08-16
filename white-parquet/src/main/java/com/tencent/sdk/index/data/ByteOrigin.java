package com.tencent.sdk.index.data;

/*****
 * 
 * @author kaynewu
 *
 * 2019年7月30日
 */
public class ByteOrigin extends OriginData<byte[]>{
	private static final long serialVersionUID = 2720326256503252976L;

	public ByteOrigin(String tablename, byte[] data) {
		super(tablename, data);
	}

}
