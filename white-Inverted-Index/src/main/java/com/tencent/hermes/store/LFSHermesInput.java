package com.tencent.hermes.store;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class LFSHermesInput extends HermesInput{
	
	private BufferedInputStream input;
	private File inputFile;
	private long bytesRead = 0L;
	
	public LFSHermesInput(File inputFile) throws IOException {
		super("LFSHermesInput(path=\"" + inputFile.getAbsolutePath() + "\")");
		this.inputFile = inputFile;
		this.input = new BufferedInputStream(new FileInputStream(inputFile));
	}

	@Override
	public void close() throws IOException {
		try {
			this.input.close();
		} catch (Exception e) {
		}
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
	    this.input.skip(skip);
	}

	@Override
	public long length() {
		return this.inputFile.length();
	}

	@Override
	public HermesInput slice(String sliceDescription, long offset, long length) throws IOException {
		return new SlicedIndexInput(sliceDescription, this, offset, length);
	}

	@Override
	public byte readByte() throws IOException {
		byte[] b = new byte[1];
		this.input.read(b);
		bytesRead ++;
		return b[0];
	}

	@Override
	public void readBytes(byte[] b, int offset, int len) throws IOException {
		this.input.read(b, offset, len);
		bytesRead += len;
	}
	
	public static final class SlicedIndexInput extends HermesInput{
		
		RandomAccessFile randomAccess;
		
		long offset;
		long length;
		
		long pointer;

		protected SlicedIndexInput(String sliceDescription, LFSHermesInput input,
				long offset, long length) throws IOException {
			super(sliceDescription);
			this.randomAccess = new RandomAccessFile(input.inputFile, "r");
			this.offset = offset;
			this.pointer = offset;
			this.length = length;
			this.randomAccess.seek(offset);
		}

		@Override
		public void close() throws IOException {
			try {
				this.randomAccess.close();
			} catch (Exception e) {
			}
		}

		@Override
		public long getFilePointer() {
			return pointer;
		}

		@Override
		public void seek(long pos) throws IOException {
			if (pos > length) {
			      throw new IllegalStateException(getClass() + " cannot seek (pos=" + pos + " length=" + length + ")");
			}
			this.randomAccess.seek(offset + pos);
			pointer = offset + pos;
		}

		@Override
		public long length() {
			return length;
		}

		@Override
		public HermesInput slice(String sliceDescription, long offset, long length) throws IOException {
			throw new IOException(getClass() + " not support slice");
		}

		@Override
		public byte readByte() throws IOException {
			return this.randomAccess.readByte();
		}

		@Override
		public void readBytes(byte[] b, int offset, int len) throws IOException {
			this.randomAccess.read(b, offset, len);
		}
		
	}

}
