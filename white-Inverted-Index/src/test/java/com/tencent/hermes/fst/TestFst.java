package com.tencent.hermes.fst;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import com.tencent.hermes.TestCaseBase;
import com.tencent.hermes.store.hdfs.HdfsHermesInput;
import com.tencent.hermes.store.hdfs.HdfsHermesOutput;
import com.tencent.hermes.store.ram.RAMOutputStream;
import com.tencent.hermes.store.utils.NumberToByteUtils;


public class TestFst extends TestCaseBase{
	
	public static String TEST_DATA_DIR = "D:/test/hermesio/hdfs";
	

	
	public static void main(String[] args) {
		buildFst(); 
	}
	
	
	public static void buildFst() {

			try {
				long[] arr = new long[] {123,565,567,34534,56761};
				String[] inputValues = new String[arr.length];
				for (int i = 0; i < arr.length; i++) {
					inputValues[i] = new String(NumberToByteUtils.long2Bytes(arr[i]));
				}
				Arrays.sort(inputValues);
				int index = 4;
				//String inputValues[] = { "cat", "deep", "do", "dog", "dogs" };
				long outputValues[] = { 5, 7, 17, 18, 21 };
				PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
				Builder<Long> builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
				BytesRef scratchBytes = null;
				IntsRefBuilder scratchInts = new IntsRefBuilder();
				for (int i = 0; i < inputValues.length; i++) {
					scratchBytes = new BytesRef(inputValues[i]);
					builder.add(Util.toIntsRef(scratchBytes, scratchInts), outputValues[i]);
				}
				FST<Long> fst = builder.finish();
				
				Long value = Util.get(fst, new BytesRef(NumberToByteUtils.long2Bytes(arr[index])));
				System.out.println(value);  
				
				RAMOutputStream ramOutput = new RAMOutputStream();
				fst.save(ramOutput);
				String filename = System.currentTimeMillis() + "fst.hermes";
				HdfsHermesOutput hdfsOutput = new HdfsHermesOutput(new Configuration(),
						new Path(TEST_DATA_DIR, filename));
				ramOutput.writeTo(hdfsOutput);
				
				ramOutput.close();
				hdfsOutput.close();
				
				HdfsHermesInput hinput = new HdfsHermesInput(new Configuration(), 
						new Path(TEST_DATA_DIR, filename), HdfsHermesOutput.CHUNK_SIZE);
				
				FST<Long> fst2 = new FST<Long>(hinput, PositiveIntOutputs.getSingleton());
				
			    value = Util.get(fst2, new BytesRef(NumberToByteUtils.long2Bytes(arr[index])));
				System.out.println(value);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

}
