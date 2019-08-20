package com.tencent.hermes.store.utils;


/****************
 * 
 * @return
 */
public class BytesRefUtils {
	private static ThreadLocal<CharsRefBuilder> CHAR_REF_BUFFER = new ThreadLocal<CharsRefBuilder>();
	public static CharsRefBuilder getcharsRef()
	{
		CharsRefBuilder sdf=CHAR_REF_BUFFER.get();
		if(sdf==null)
		{
			sdf=new CharsRefBuilder();
		}
		return sdf;
	}
	
	
	public static String byteRefToReadable(BytesRef bRef) {
		CharsRefBuilder builder = getcharsRef();
		builder.copyUTF8Bytes(bRef);
		return builder.get().toString();
	}

}
