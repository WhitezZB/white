package com.tencent.hermes.store;


import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.tencent.hermes.store.hdfs.HdfsHermesInput;
import com.tencent.hermes.store.hdfs.HdfsHermesOutput;
import com.tencent.hermes.store.ram.RAMOutputStream;

import junit.framework.TestCase;

public class HdfsInOutTests extends TestCase{
	static {
		System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.7.1");
	}
	
	
	public static void testHdfsIo() throws Exception {
		HdfsHermesOutput houtput = new HdfsHermesOutput(new Configuration(),
				new Path("D:/test/hermesio/hdfs/data.hermes"));
		houtput.writeString("love china");
		houtput.writeString("love tencent");
		houtput.writeInt(10);
		houtput.writeLong(9l);
		houtput.close();
		
		HdfsHermesInput hinput = new HdfsHermesInput(new Configuration(), 
				new Path("D:/test/hermesio/hdfs/data.hermes"), HdfsHermesOutput.CHUNK_SIZE);
		String s1 = hinput.readString();
		String s2 = hinput.readString();
		int a1 = hinput.readInt();
		long a2 = hinput.readLong();
		hinput.close();
		System.out.println(s1 + " " + s2 + " " + a1 + " " + a2);
	}
	
	public static void testDirectoryReader() throws IOException {
		int length = 10000;
		long[] data = new long[length];
		long seed = System.currentTimeMillis();
		Random random = new Random(seed);
		for (int i = 0; i < data.length; i++) {
			data[i] = Math.abs(random.nextLong()%100000000); 
		}
		int bitRequired;
		Arrays.sort(data);
		System.out.println(Arrays.toString(data));
		bitRequired = DirectWriter.unsignedBitsRequired(data[data.length -1]);
		System.out.println(bitRequired);
		long minVal = data[0];
		long[] dataNew = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			dataNew[i] = data[i] - minVal;
		}
		//System.out.println(Arrays.toString(dataNew));
		bitRequired = DirectWriter.unsignedBitsRequired(dataNew[data.length -1]);
		System.out.println(bitRequired);
		
		long[] delta = new long[data.length];
		delta[0] = 0;
		long max = Long.MIN_VALUE;
		for (int i = 1; i < data.length; i++) {
			delta[i] = dataNew[i] - dataNew[i -1];
			max  = max < delta[i] ? delta[i] : max;
		}
		//System.out.println(Arrays.toString(delta));		
		bitRequired = DirectWriter.unsignedBitsRequired(max);
		System.out.println(bitRequired);
		
		HdfsHermesOutput output = new HdfsHermesOutput(new Configuration(),
				new Path("D:/test/hermesio/hdfs/" +seed+ ".a.hermes"));
		output.writeLong(minVal);
		output.writeInt(bitRequired);
		output.writeInt(delta.length);
		
		DirectWriter dw = DirectWriter.getInstance(output, delta.length, bitRequired);
		for (int i = 0; i < delta.length; i++) {
			dw.add(delta[i]);
		}
		dw.finish();
		System.out.println("point:" + output.getFilePointer());	
		output.writeLong(output.getFilePointer());
		output.close();
		
		HdfsHermesInput input = new HdfsHermesInput(new Configuration(), 
				new Path("D:/test/hermesio/hdfs/" + seed + ".a.hermes"), HdfsHermesOutput.CHUNK_SIZE);
		minVal = input.readLong();
		bitRequired = input.readInt();
		long valueNumbers = input.readInt();
		long value = minVal;
		int valueLength = (int)Math.ceil((valueNumbers * bitRequired)/8 + 3);
		RandomAccessInput slice =  (RandomAccessInput) input.randomAccessSlice(input.getFilePointer(),valueLength);
		LongValues longValues = DirectReader.getInstance(slice, bitRequired);
		long[] read = new long[data.length];
		for (int i = 0; i < valueNumbers; i++) {
			value += longValues.get(i);
			read[i] = value; 
		}
		System.out.println(Arrays.toString(read));	
		assert Arrays.equals(data, read);
		input.skipBytes(valueLength);
		long pointer = input.readLong();
		System.out.println("point:" + pointer);	
		input.close();
	}
	
	
	public static void testRamOutput() throws IOException {
		int length = 100;
		long[] data = new long[length];
		long seed = System.currentTimeMillis();
		Random random = new Random(seed);
		for (int i = 0; i < data.length; i++) {
			data[i] = Math.abs(random.nextLong()%100000000); 
		}
		int bitRequired;
		Arrays.sort(data);
		System.out.println(Arrays.toString(data));
		bitRequired = DirectWriter.unsignedBitsRequired(data[data.length -1]);
		System.out.println(bitRequired);
		long minVal = data[0];
		long[] dataNew = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			dataNew[i] = data[i] - minVal;
		}
		//System.out.println(Arrays.toString(dataNew));
		bitRequired = DirectWriter.unsignedBitsRequired(dataNew[data.length -1]);
		System.out.println(bitRequired);
		
		long[] delta = new long[data.length];
		delta[0] = 0;
		long max = Long.MIN_VALUE;
		for (int i = 1; i < data.length; i++) {
			delta[i] = dataNew[i] - dataNew[i -1];
			max  = max < delta[i] ? delta[i] : max;
		}
		//System.out.println(Arrays.toString(delta));		
		bitRequired = DirectWriter.unsignedBitsRequired(max);
		System.out.println(bitRequired);
		
		RAMOutputStream output = new RAMOutputStream();
		output.writeLong(minVal);
		output.writeInt(bitRequired);
		output.writeInt(delta.length);
		
		DirectWriter dw = DirectWriter.getInstance(output, delta.length, bitRequired);
		for (int i = 0; i < delta.length; i++) {
			dw.add(delta[i]);
		}
		dw.finish();
		System.out.println("point:" + output.getFilePointer());	
		output.writeLong(output.getFilePointer());	
		HermesOutput hdfsOutput = new HdfsHermesOutput(new Configuration(),
				new Path("D:/test/hermesio/hdfs/" +seed+ ".ram.hermes"));
		output.writeTo(hdfsOutput);
		output.close();	
		hdfsOutput.close();
		
		
		HdfsHermesInput input = new HdfsHermesInput(new Configuration(), 
				new Path("D:/test/hermesio/hdfs/" + seed + ".ram.hermes"), HdfsHermesOutput.CHUNK_SIZE);
		minVal = input.readLong();
		bitRequired = input.readInt();
		long valueNumbers = input.readInt();
		long value = minVal;
		int valueLength = (int)Math.ceil((valueNumbers * bitRequired)/8 + 3);
		RandomAccessInput slice =  (RandomAccessInput) input.randomAccessSlice(input.getFilePointer(),valueLength);
		LongValues longValues = DirectReader.getInstance(slice, bitRequired);
		long[] read = new long[data.length];
		for (int i = 0; i < valueNumbers; i++) {
			value += longValues.get(i);
			read[i] = value; 
		}
		System.out.println(Arrays.toString(read));	
		assert Arrays.equals(data, read);
		input.skipBytes(valueLength);
		long pointer = input.readLong();
		System.out.println("point:" + pointer);	
		input.close();
	}

}
