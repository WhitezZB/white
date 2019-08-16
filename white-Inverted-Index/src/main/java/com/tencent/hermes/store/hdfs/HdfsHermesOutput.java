package com.tencent.hermes.store.hdfs;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.tencent.hermes.store.HermesOutput;

/*****************
 * hdfs output
 * @author kaynewu
 *
 * 2019年8月15日
 */
public class HdfsHermesOutput extends HermesOutput{
	
	private static final Logger LOG = Logger.getLogger(HdfsHermesOutput.class);
	Path path;
	
	private final CRC32 crc = new CRC32();
	private final CheckedOutputStream os;
	  
	private long bytesWritten = 0L;

	public HdfsHermesOutput(Configuration conf, Path path) throws IOException {
		super("HdfsHermesOutput=" + path, path.getName());
		FileSystem fileSystem = path.getFileSystem(conf);
		this.os = new CheckedOutputStream(new BufferedOutputStream(
				fileSystem.create(path, true), HermesOutput.CHUNK_SIZE), crc);
		this.path = path;
	}

	@Override
	public void close() throws IOException {
		 this.os.close();
	}

	@Override
	public long getFilePointer() {
		return bytesWritten;
	}

	@Override
	public long getChecksum() throws IOException {
		LOG.info("getChecksum=" +this.path+","+bytesWritten);
	    return crc.getValue();
	}

	@Override
	public void writeByte(byte b) throws IOException {
		os.write(b);
	    bytesWritten++;	
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) throws IOException {
		os.write(b, offset, length);
	    bytesWritten += length;
	}
	 

}
