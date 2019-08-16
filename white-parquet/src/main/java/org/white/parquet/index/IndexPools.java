package org.white.parquet.index;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.white.parquet.ProtoWriter;
import org.white.parquet.utils.HadoopUtils;

import com.tencent.dp.gdt.utils.WechatTrainingDataProtos.WechatTrainingData;

public class IndexPools {

	public static void main(String[] args) throws Exception {
		String hadoopConf = args[0];
		int poolsize = Integer.parseInt(args[1]);
		Configuration conf = HadoopUtils.grabConfiguration(hadoopConf, new Configuration());
		String outpuPath = args[2];
		String inputDir = args[3];
		Path path = new Path(outpuPath);
		FileSystem fs = path.getFileSystem(conf);
		List<IndexRunner> ilists = new ArrayList<IndexRunner>();
		for (int i = 0; i < poolsize; i++) {
			Path basePath = new Path(path, "part-" + i);
			if(!fs.exists(basePath)) {
				fs.mkdirs(basePath);
			}
			Path parquetFile = new Path(basePath,"data.parquet");
			ProtoWriter protoWriter = new ProtoWriter(WechatTrainingData.class, parquetFile.toString(), 
					conf,CompressionCodecName.UNCOMPRESSED);
			IndexRunner runner = new IndexRunner(protoWriter);
			ilists.add(runner);
			ThreadPools.submit(runner);
		}
		FeedFile(inputDir, ilists, new AtomicInteger(0));
		for (IndexRunner ir :ilists) {
			ir.stop();
		}
	}
	
	public static void FeedFile(String inputDir,List<IndexRunner> lindexs,AtomicInteger index) {
		File file = new File(inputDir);
		if(file.isFile() && !file.getName().endsWith("check")) {
			int idx = index.incrementAndGet() % lindexs.size();
			lindexs.get(idx).addSource(file.getAbsolutePath());
		}else if(file.isDirectory()) {
			File[] flist = file.listFiles();
			for (int i = 0; i < flist.length; i++) {
				FeedFile(flist[i].getAbsolutePath(), lindexs, index);
			}
		}
	}

}
