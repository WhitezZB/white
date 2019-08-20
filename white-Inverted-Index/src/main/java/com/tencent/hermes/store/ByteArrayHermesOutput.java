package com.tencent.hermes.store;

import java.io.IOException;

/************************************'
 * 
 * @author kaynewu
 *
 * 2019年8月19日
 */
public class ByteArrayHermesOutput extends HermesOutput {

	private byte[] bytes;
	
	private int pos;
	private int limit;

	public ByteArrayHermesOutput(byte[] bytes) {
		super("ByteArrayHermesOutput", "ram");
		this.reset(bytes);
	}
	
	public ByteArrayHermesOutput(byte[] bytes, int offset, int len) {
		super("ByteArrayHermesOutput", "ram");
	    reset(bytes, offset, len);
	}

	public void reset(byte[] bytes) {
		reset(bytes, 0, bytes.length);
	}

	public void reset(byte[] bytes, int offset, int len) {
		this.bytes = bytes;
		pos = offset;
		limit = offset + len;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public long getFilePointer() {
		return this.pos;
	}

	@Override
	public long getChecksum() throws IOException {
		return 0;
	}

	@Override
	public void writeByte(byte b) throws IOException {
		assert pos < limit;
	    bytes[pos++] = b;
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		assert pos + length <= limit;
	    System.arraycopy(b, offset, bytes, pos, length);
	    pos += length;
	}

}
