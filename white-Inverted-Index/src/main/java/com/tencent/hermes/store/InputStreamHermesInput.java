package com.tencent.hermes.store;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;



public class InputStreamHermesInput extends HermesInput{
	
	  private final CRC32 crc = new CRC32();
	  private final BufferedInputStream os;
	  
	  private long bytesRead = 0L;
	  

	protected InputStreamHermesInput(String resourceDescription, InputStream in, int bufferSize) {
		super(resourceDescription);
		this.os = new BufferedInputStream(new CheckedInputStream(in, crc), bufferSize);
	}

	@Override
	public void close() throws IOException {
		this.os.close();
	}

	@Override
	public long getFilePointer() {
		return bytesRead;
	}

	@Override
	public void seek(long pos) throws IOException {
	    final long curFP = getFilePointer();
	    final long skip = pos - curFP;
	    if (skip < 0) {
	      throw new IllegalStateException(getClass() + " cannot seek backwards (pos=" + pos + " getFilePointer()=" + curFP + ")");
	    }
	    this.os.skip(skip);
	}

	@Override
	public long length() {
		return 0;
	}


	@Override
	public byte readByte() throws IOException {
		byte[] b = new byte[1];
		os.read(b);
		bytesRead ++;
		return b[0];
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		this.os.read(b, offset, len);
		bytesRead += len;
	}

	@Override
	public HermesInput slice(String sliceDescription, long offset, long length) throws IOException {
		return null;
	}

}
