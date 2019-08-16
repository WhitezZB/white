package org.white.parquet.merge;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;


public class ProtoParquetMerge implements PMerge{
	

	@Override
	public boolean merge(Configuration conf, Path outputFile, Path... inputFiles) throws Exception {
		 OutputFile output = HadoopOutputFile.fromPath(outputFile, conf);
		 FileMetaData metaData = ParquetFileWriter.mergeMetadataFiles(Arrays.asList(inputFiles), conf).getFileMetaData();
		 ParquetFileWriter writer = new ParquetFileWriter(output, metaData.getSchema(), Mode.CREATE, 
				 ParquetWriter.DEFAULT_BLOCK_SIZE * 8, ParquetWriter.MAX_PADDING_SIZE_DEFAULT);
		 writer.start();
		 for (int i = 0; i < inputFiles.length; i++) {
			writer.appendFile(HadoopInputFile.fromPath(inputFiles[i], conf));
		}
		writer.end(metaData.getKeyValueMetaData());
		return true;
	}

	public static void main(String[] args) throws Exception {
		Path output = new Path(args[0]);
		List<Path> inputPaths = new ArrayList<Path>();
		for (int i = 1; i < args.length; i++) {
			inputPaths.add(new Path(args[i]));
		}
		
		ProtoParquetMerge merge = new ProtoParquetMerge();
		merge.merge(new Configuration(), output, inputPaths.toArray(new Path[inputPaths.size()]));
	}
}
