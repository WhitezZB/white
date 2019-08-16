package com.tencent.hermes.index.test;

import java.io.File;
import java.util.Random;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

public class TestUtils {
	   private static Random random = new Random(System.currentTimeMillis());
	  
	  public static Random random() {
		    return random;
	  }
	  
	  
	  public static Directory newFsDirectory(String indexDir) throws Exception{
		  Directory dir = FSDirectory.open(new File(indexDir).toPath());
		  return dir;
	  }
	  
	  public static Directory newRamDirectory() throws Exception{
		  return new RAMDirectory();
	  }
}
