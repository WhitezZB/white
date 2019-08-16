package com.tencent.hermes.store;

import java.io.File;
import java.io.IOException;

public class FSHermesOutputTests {
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.7.1");
		testLocalOutInput(); 
	}
	
	
	public static void testLocalOutInput() throws IOException {
		LFSHermesOutput output = new LFSHermesOutput(new File("D:\\test\\hermesio\\local.out.hermes"));
		output.writeInt(10);
		output.writeString("love tencent");
		output.close();
		
		LFSHermesInput input = new LFSHermesInput(new File("D:\\test\\hermesio\\local.out.hermes"));
		//int a = input.readInt();
		HermesInput slice = input.slice("slice", 4, input.length() - 4);
		String s = slice.readString();
		System.out.println( s);
		input.close();
		slice.close();
	}
	
	public static void testCheckSumOutInput() throws IOException {
		HermesOutput output = new FSCheckSumHermesOutput("out.hermes", new File("D:\\test\\hermesio").toPath());
		output.writeInt(10);
		output.writeString("love tencent");
		output.close();
		
		HermesInput input = new FSCheckSumHermesInput("out.hermes", new File("D:\\test\\hermesio").toPath());
		int a = input.readInt();
		String s = input.readString();
		System.out.println(a + ":" + s);
		input.close();
	}

}
