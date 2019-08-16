package org.white.parquet;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

public class ProtoParquetWriter<T extends MessageOrBuilder> extends ParquetWriter<T>{
	
	  /**
	   * Create a new {@link ProtoParquetWriter}.
	   *
	   * @param file                 The file name to write to.
	   * @param protoMessage         Protobuf message class
	   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
	   * @param blockSize            HDFS block size
	   * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
	   * @throws IOException if there is an error while writing
	   */
	  @SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
	public ProtoParquetWriter(Path file, Configuration conf,Class<? extends Message> protoMessage,
	                            CompressionCodecName compressionCodecName, int blockSize,
	                            int pageSize) throws IOException {
	    super(file, new ProtoWriteSupport(protoMessage),
	            compressionCodecName, blockSize, pageSize);
	  }

	  /**
	   * Create a new {@link ProtoParquetWriter}.
	   *
	   * @param file                 The file name to write to.
	   * @param protoMessage         Protobuf message class
	   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
	   * @param blockSize            HDFS block size
	   * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
	   * @param enableDictionary     Whether to use a dictionary to compress columns.
	   * @param validating           to turn on validation using the schema
	   * @throws IOException if there is an error while writing
	   */
	  @SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
	public ProtoParquetWriter(Path file,Configuration conf, Class<? extends Message> protoMessage,
	                            CompressionCodecName compressionCodecName, int blockSize,
	                            int pageSize, boolean enableDictionary, boolean validating) throws IOException {
	    super(file, new ProtoWriteSupport(protoMessage),
	            compressionCodecName, blockSize, pageSize, enableDictionary, validating);
	  }

	  /**
	   * Create a new {@link ProtoParquetWriter}. The default block size is 50 MB.The default
	   * page size is 1 MB.  Default compression is no compression. (Inherited from {@link ParquetWriter})
	   *
	   * @param file The file name to write to.
	   * @param protoMessage         Protobuf message class
	   * @throws IOException if there is an error while writing
	   */
	  public ProtoParquetWriter(Path file,Configuration conf, Class<? extends Message> protoMessage) throws IOException {
	    this(file, conf, protoMessage, CompressionCodecName.UNCOMPRESSED,
	            DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
	  }
	  
	  
	  public static Builder builder(Path file) {
		    return new Builder(file);
	  }
	  
	  public static class Builder extends ParquetWriter.Builder<MessageOrBuilder, Builder> {
		    private Class<? extends Message>  protoMessage = null;

		    private Builder(Path file) {
		      super(file);
		    }

		    public Builder withMessage(Class<? extends Message>  protoMessage) {
		      this.protoMessage = protoMessage;
		      return this;
		    }

		    @Override
		    protected Builder self() {
		      return this;
		    }

		    @Override
		    protected WriteSupport<MessageOrBuilder> getWriteSupport(Configuration conf) {
		      return new ProtoWriteSupport(protoMessage);
		    }

		}

}
