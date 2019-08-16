package org.white.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.white.parquet.utils.ParquetUtils;


/**********
 * 
 * @author kaynewu
 *
 * 2019年7月22日
 */
public class SimpleWriter extends PWriter{
	
	ParquetWriter<Group> writer;
	SimpleGroupFactory groupFactory;

	
	public  SimpleWriter(String schemaStr,String writePath,Configuration conf, CompressionCodecName compressionCodec,
			ParquetFileWriter.Mode mode,ParquetProperties.WriterVersion version) throws IOException {
		Path file = new Path(ParquetUtils.formatPartFile(writePath));
		MessageType schema = PSchema.parseParquetSchema(schemaStr);
		ExampleParquetWriter.Builder builder = ExampleParquetWriter
				.builder(file).withWriteMode(mode)
				.withWriterVersion(version)
				.withCompressionCodec(compressionCodec)
				.withConf(conf)
				.withType(schema);
		writer = builder.build();
		groupFactory = new SimpleGroupFactory(schema);
		this.path = writePath;
		this.conf = conf;
		
	}
	
	public  SimpleWriter(String schemaStr,String writePath,Configuration conf) throws IOException {
		this(schemaStr, writePath,conf, CompressionCodecName.SNAPPY, 
				ParquetFileWriter.Mode.OVERWRITE, ParquetProperties.WriterVersion.PARQUET_2_0);
	}
	
	public void writer(Group group) throws IOException {
		writer.write(group);
	}
	
	public void writer(Map<String,Object> record) throws IOException {
		if(record != null && record.size() > 0) {
			Group group = groupFactory.newGroup();
			for (String key : record.keySet()) {
				ParquetUtils.addGroup(group, key, record.get(key));
			}
			this.writer(group);
		}		
	}

	@Override
	public void close() {
		try {
			writer.close();
		} catch (Exception e) {
		}
	}

	@Override
	public void write(Object t) throws IOException {
		if(t instanceof Group) {
			this.writer((Group)t);
			this.docCount ++;
		}
	}
	
	public static void main(String[] args) throws Exception {
		SimpleWriter spw = new SimpleWriter("message IdWeightString {\n" + 
				"        optional binary id (UTF8);\n" + 
				"        optional double weight;\n" + 
				"    }", args[0], new Configuration());
		for (int i = 0; i < 100; i++) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("id", UUID.randomUUID().toString());
			map.put("weight", new Double(i));
			spw.writer(map);
		}		
		spw.close();	
	}


}
