package org.white.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.proto.ProtoParquetReader;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.googlecode.protobuf.format.JsonFormat;
import com.tencent.dp.gdt.utils.WechatTrainingDataProtos;

public class ProtoReader implements PReader<MessageOrBuilder>{
	
	ParquetReader<MessageOrBuilder> reader;
	
	public  ProtoReader(Path path,Configuration conf) throws IOException {
		Builder<MessageOrBuilder> builder =   ProtoParquetReader.builder(path);
		builder.withConf(conf).withFilter(FilterCompat.NOOP);
		this.reader = builder.build();
		
	}

	@Override
	public MessageOrBuilder read() throws IOException {
		return this.reader.read();
	}

	public static void main(String[] args) throws IllegalArgumentException, IOException {
		ProtoReader preader = new ProtoReader(new Path(args[0]), new Configuration());
		MessageOrBuilder line = preader.read();
		JsonFormat jf = new JsonFormat();
		while(line != null) {
			WechatTrainingDataProtos.WechatTrainingData message;
			if(line instanceof WechatTrainingDataProtos.WechatTrainingData) {
				message = (WechatTrainingDataProtos.WechatTrainingData)line;
			}else {
				message = ((WechatTrainingDataProtos.WechatTrainingData.Builder)line).build();
			}
			String jsonFormat = jf.printToString(line.getDefaultInstanceForType());
			System.out.println(jsonFormat);
			line = preader.read();
		}
	}
}
