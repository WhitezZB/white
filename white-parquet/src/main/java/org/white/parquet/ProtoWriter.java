package org.white.parquet;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.white.parquet.utils.ParquetUtils;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;


public class ProtoWriter extends PWriter{
	ParquetWriter<MessageOrBuilder> writer;
	Class<? extends Message> protoMessage;
	
	
	public  ProtoWriter(Class<? extends Message> protoMessage,String writePath,
			Configuration conf, CompressionCodecName compressionCodec,
			ParquetFileWriter.Mode mode,ParquetProperties.WriterVersion version) throws IOException {
		Path file = new Path(ParquetUtils.formatPartFile(writePath));
		ProtoParquetWriter.Builder builder = ProtoParquetWriter
				.builder(file).withWriteMode(mode)
				.withWriterVersion(version)
				.withCompressionCodec(compressionCodec)
				.withMessage(protoMessage);
		if(conf != null) {
			builder.withConf(conf);
		}
		this.protoMessage = protoMessage;
		writer = builder.build();	
		this.path = writePath;
		this.conf = conf;
	}
	
	public  ProtoWriter(Class<? extends Message> protoMessage,String writePath, Configuration conf) throws IOException {
		this(protoMessage, writePath, conf, CompressionCodecName.SNAPPY, 
				ParquetFileWriter.Mode.OVERWRITE, ParquetProperties.WriterVersion.PARQUET_2_0);
	}
	
	public  ProtoWriter(Class<? extends Message> protoMessage,String writePath, Configuration conf ,CompressionCodecName compressionCodec) throws IOException {
		this(protoMessage, writePath, conf, compressionCodec, 
				ParquetFileWriter.Mode.OVERWRITE, ParquetProperties.WriterVersion.PARQUET_2_0);
	}


	@Override
	public void close() {
		IOUtils.closeQuietly(this.writer);
	}


	@Override
	public void write(Object t) throws IOException {
		if(t instanceof MessageOrBuilder) {
			this.writer.write((MessageOrBuilder)t);
			this.docCount ++;
		}
	}
}
