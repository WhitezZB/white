package com.tencent.hermes.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class LFSHermesOutput extends HermesOutput{
	
	BufferedOutputStream output; 
	private long bytesWritten = 0L;

	protected LFSHermesOutput(File outputFile) throws IOException {
		super("LFSHermesOutput(path=\"" + outputFile.getAbsolutePath() + "\")",
				outputFile.getName());
		this.output = new BufferedOutputStream(new FileOutputStream(outputFile));
	}

	@Override
	public void close() throws IOException {
		try {
			this.output.close();
		} catch (Exception e) {
		}
	}

	@Override
	public long getFilePointer() {
		return bytesWritten;
	}

	@Override
	public void writeByte(byte b) throws IOException {
		this.output.write(b);
		bytesWritten ++;
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		this.output.write(b, offset, length);
		bytesWritten += length;
	}

	@Override
	public long getChecksum() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
