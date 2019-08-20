package com.tencent.hermes.store;

import java.io.IOException;

/************
 * 
 * @author kaynewu
 *
 * 2019年8月19日
 */
public class ByteArrayHermesInput extends HermesInput{
	 private byte[] bytes;

	  //private int offset;
	  private int length;
	  
	  private int pos;

	public ByteArrayHermesInput(byte[] bytes) {
		super("ByteArrayHermesInput");
		this.bytes = bytes;
		this.pos = 0;
		this.length = bytes.length;
	}
	
	public ByteArrayHermesInput(byte[] bytes, int offset, int length) {
		super("ByteArrayHermesInput");
		this.bytes = bytes;
		this.pos = offset;
		this.length = length;
	}


	@Override
	public void close() throws IOException {
	}

	@Override
	public long getFilePointer() {
		return this.pos;
	}

	@Override
	public void seek(long pos) throws IOException {
		this.pos = (int) pos;
	}

	@Override
	public long length() {
		// TODO Auto-generated method stub
		return this.length;
	}

	@Override
	public HermesInput slice(String sliceDescription, long offset, long length) throws IOException {
		return new ByteArrayHermesInput(this.bytes, (int)offset, (int)length);
	}

	@Override
	public byte readByte() throws IOException {
		return this.bytes[this.pos ++];
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		System.arraycopy(this.bytes, this.pos, b, offset, len);
		this.pos += len;
	}
	
	  @Override
	  public void skipBytes(long count) {
	    pos += count;
	  }

	  @Override
	  public short readShort() {
	    return (short) (((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF));
	  }
	 
	  @Override
	  public int readInt() {
	    return ((bytes[pos++] & 0xFF) << 24) | ((bytes[pos++] & 0xFF) << 16)
	      | ((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF);
	  }
	 
	  @Override
	  public long readLong() {
	    final int i1 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
	      ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
	    final int i2 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
	      ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
	    return (((long)i1) << 32) | (i2 & 0xFFFFFFFFL);
	  }

	  @Override
	  public int readVInt() {
	    byte b = bytes[pos++];
	    if (b >= 0) return b;
	    int i = b & 0x7F;
	    b = bytes[pos++];
	    i |= (b & 0x7F) << 7;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7F) << 14;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7F) << 21;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
	    i |= (b & 0x0F) << 28;
	    if ((b & 0xF0) == 0) return i;
	    throw new RuntimeException("Invalid vInt detected (too many bits)");
	  }
	 
	  @Override
	  public long readVLong() {
	    byte b = bytes[pos++];
	    if (b >= 0) return b;
	    long i = b & 0x7FL;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 7;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 14;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 21;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 28;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 35;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 42;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 49;
	    if (b >= 0) return i;
	    b = bytes[pos++];
	    i |= (b & 0x7FL) << 56;
	    if (b >= 0) return i;
	    throw new RuntimeException("Invalid vLong detected (negative values disallowed)");
	  }

}
