package com.tencent.hermes.store.utils;

public class NumberToByteUtils {
	
	public static byte[] int2Bytes(int value) {
		byte[] result = new byte[4];
	    result[0] =   (byte) (value >> 24);
	    result[1] = (byte) (value >> 16);
	    result[2] = (byte) (value >>  8);
	    result[3] = (byte) value;
	    return result;
	}
	
	public static int  bytes2Int(byte[] encoded, int offset) {
	    int x = ((encoded[offset] & 0xFF) << 24)   | 
	            ((encoded[offset+1] & 0xFF) << 16) |
	            ((encoded[offset+2] & 0xFF) <<  8) | 
	             (encoded[offset+3] & 0xFF);
	    return x;
	}
	
	public static byte[] long2Bytes(long value) {
		byte[] result = new byte[8];
	    result[0] =   (byte) (value >> 56);
	    result[1] = (byte) (value >> 48);
	    result[2] = (byte) (value >> 40);
	    result[3] = (byte) (value >> 32);
	    result[4] = (byte) (value >> 24);
	    result[5] = (byte) (value >> 16);
	    result[6] = (byte) (value >> 8);
	    result[7] = (byte) value;
	    return result;
	}
	
	 public static long bytes2Long(byte[] encoded, int offset) {
		    long v = ((encoded[offset] & 0xFFL) << 56)   |
		             ((encoded[offset+1] & 0xFFL) << 48) |
		             ((encoded[offset+2] & 0xFFL) << 40) |
		             ((encoded[offset+3] & 0xFFL) << 32) |
		             ((encoded[offset+4] & 0xFFL) << 24) |
		             ((encoded[offset+5] & 0xFFL) << 16) |
		             ((encoded[offset+6] & 0xFFL) << 8)  |
		              (encoded[offset+7] & 0xFFL);
		    return v;
	 }
	 
	 
	 
	 public static byte[] double2Byte(double value) {
	     long v = Double.doubleToLongBits(value);
	     return long2Bytes(v);
	 }
	  

	 public static byte[] float2Bytes(float value) {
	     int v = Float.floatToIntBits(value);
	     return int2Bytes(v);
	 }

}
