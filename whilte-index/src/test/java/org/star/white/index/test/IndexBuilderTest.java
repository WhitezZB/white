package org.star.white.index.test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.star.whilte.index.util.DocumentBuilder;
import org.star.whilte.schema.WhiteSchema;

public class IndexBuilderTest {
	
	public static void main(String[] args) throws IOException {
		WhiteSchema schema = WhiteSchema.LOCAL(new Path("D:\\test\\lucene\\schema.xml"));
		Directory dir = FSDirectory.open(Paths.get("D:\\test\\lucene\\luceneIndex"));
		IndexWriterConfig iwc = new IndexWriterConfig(schema.getIndexAnalyzer());
		iwc.setUseCompoundFile(false);
        IndexWriter writer = new IndexWriter(dir, iwc);
        
        Map<String,Object> m1 = new HashMap<String,Object>();
        m1.put("name", "kaynewu");
        m1.put("age", 28);
        m1.put("number", 18575525829l);
        writer.addDocument(DocumentBuilder.toDocument(m1, schema));
        
        m1 = new HashMap<String,Object>();
        m1.put("name", "grace");
        m1.put("age", 29);
        m1.put("number", 18575565829l);
        writer.addDocument(DocumentBuilder.toDocument(m1, schema));
        
        m1 = new HashMap<String,Object>();
        m1.put("name", "wayne");
        m1.put("age", 40);
        m1.put("number", 18575525821l);
        writer.addDocument(DocumentBuilder.toDocument(m1, schema));
        
        m1 = new HashMap<String,Object>();
        m1.put("name", "brouce");
        m1.put("age", 99);
        m1.put("number", 1857445525829l);
        writer.addDocument(DocumentBuilder.toDocument(m1, schema));
        
        m1 = new HashMap<String,Object>();
        m1.put("name", "like");
        m1.put("age", 55);
        m1.put("number", 18575522729l);
        writer.addDocument(DocumentBuilder.toDocument(m1, schema));
        
        writer.forceMerge(1);
        writer.close();
	}

}
