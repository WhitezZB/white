package com.tencent.hermes.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.tencent.hermes.store.LFSHermesInput.SlicedIndexInput;

public class FSTestDirectorWriterAndReader {
	
	public static void main(String[] args) throws IOException {
		System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.7.1");
		testDirectoryReader();
	}
	
	
	public static void testDirectoryReader() throws IOException {
		long[] data = new long[] {23,34,53,44,67,56,71,23,34,56};
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
		System.out.println(Arrays.toString(dataNew));
		bitRequired = DirectWriter.unsignedBitsRequired(dataNew[data.length -1]);
		System.out.println(bitRequired);
		
		long[] delta = new long[data.length];
		delta[0] = 0;
		for (int i = 1; i < data.length; i++) {
			delta[i] = dataNew[i] - dataNew[i -1];
		}
		System.out.println(Arrays.toString(delta));		
		bitRequired = DirectWriter.unsignedBitsRequired(delta[data.length -1]);
		System.out.println(bitRequired);
		
		LFSHermesOutput output = new LFSHermesOutput(new File("D:\\test\\hermesio\\data.hermes"));
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
		
		LFSHermesInput input = new LFSHermesInput(new File("D:\\test\\hermesio\\data.hermes"));
		minVal = input.readLong();
		bitRequired = input.readInt();
		long valueNumbers = input.readInt();
		long value = minVal;
		int valueLength = (int)Math.ceil((valueNumbers * bitRequired)/8 + 3);
		RandomAccessInput slice =  input.randomAccessSlice(input.getFilePointer(),valueLength);
		LongValues longValues = DirectReader.getInstance(slice, bitRequired);
		long[] read = new long[data.length];
		for (int i = 0; i < valueNumbers; i++) {
			value += longValues.get(i);
			read[i] = value; 
		}
		System.out.println(Arrays.toString(read));	
		input.skipBytes(valueLength);
		long pointer = input.readLong();
		System.out.println("point:" + pointer);	
		input.close();
	}

}
