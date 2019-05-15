package org.star.white.index.test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.star.whilte.index.util.DocumentBuilder;
import org.star.whilte.schema.WhiteSchema;

public class IndexBuilderTest {
	
	public static void main(String[] args) throws Exception {
		searchIndex();
	}
	
	public static void searchIndex() throws Exception {
		Directory dir = FSDirectory.open(Paths.get("D:\\test\\lucene\\luceneIndex-1"));
		DirectoryReader ireader = DirectoryReader.open(dir);
		IndexSearcher isearcher = new IndexSearcher(ireader);
		
		String[] multiFields = { "fieldname" };
		Analyzer analyzer = new StandardAnalyzer();
		MultiFieldQueryParser parser = new MultiFieldQueryParser( multiFields, analyzer);
		
		Query query = parser.parse("卧槽");
		int count =isearcher.count(query);
		System.out.println("count:" + count);
	}
	
	
	public static void buildIndex() throws IOException {
		Analyzer analyzer = new StandardAnalyzer();
		Directory dir = FSDirectory.open(Paths.get("D:\\test\\lucene\\luceneIndex-4"));
		
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setUseCompoundFile(false);
        IndexWriter iwriter = new IndexWriter(dir, iwc);
        
       
		String[] text = { "中华人民共和国中央人民政府", "中国是个伟大的国家", "我出生在美丽的中国，我爱中国，中国",
				"中华美丽的中国爱你", "美国跟中国式的国家", "卧槽，你是中国的" };
		
		
		FieldType TYPE_STORED = new FieldType();
		
		TYPE_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
	    TYPE_STORED.setTokenized(true);
	    TYPE_STORED.setStored(true);
	    TYPE_STORED.freeze();
		for (int i = 0; i < 100000; i++) {
			Document doc = new Document();
			doc.add(new Field("fieldname", text[i%text.length], TYPE_STORED));			
			iwriter.addDocument(doc);
		}
		 

		
		iwriter.forceMerge(1);
		iwriter.close();
		
	}
	
	public static void schemaIndex() throws IOException {
		WhiteSchema schema = WhiteSchema.LOCAL(new Path("D:\\test\\lucene\\schema.xml"));
		Directory dir = FSDirectory.open(Paths.get("D:\\test\\lucene\\luceneIndex"));
		IndexWriterConfig iwc = new IndexWriterConfig(schema.getIndexAnalyzer());
		iwc.setUseCompoundFile(false);
        IndexWriter writer = new IndexWriter(dir, iwc);
        for (int i = 0; i < 1; i++) {          
            Map<String,Object> m1 = new HashMap<String,Object>();
            m1.put("name", "kaynewu");
            m1.put("age", 28);
            m1.put("number", 18575525829l);
            m1.put("content", "深圳腾讯");
            writer.addDocument(DocumentBuilder.toDocument(m1, schema));
            
            m1 = new HashMap<String,Object>();
            m1.put("name", "grace");
            m1.put("age", 29);
            m1.put("number", 18575565829l);
            m1.put("content", "北京百度");
            writer.addDocument(DocumentBuilder.toDocument(m1, schema));
            
            m1 = new HashMap<String,Object>();
            m1.put("name", "wayne");
            m1.put("age", 40);
            m1.put("number", 18575525821l);
            m1.put("content", "杭州阿里巴巴");
            writer.addDocument(DocumentBuilder.toDocument(m1, schema));
            
            m1 = new HashMap<String,Object>();
            m1.put("name", "brouce");
            m1.put("age", 99);
            m1.put("number", 1857445525829l);
            m1.put("content", "腾讯滨海大厦");
            writer.addDocument(DocumentBuilder.toDocument(m1, schema));
            
            m1 = new HashMap<String,Object>();
            m1.put("name", "like");
            m1.put("age", 55);
            m1.put("number", 18575522729l);
            m1.put("content", "硅谷谷歌亚马逊");
            writer.addDocument(DocumentBuilder.toDocument(m1, schema));
		}

        
        writer.forceMerge(1);
        writer.close();
	}

}
