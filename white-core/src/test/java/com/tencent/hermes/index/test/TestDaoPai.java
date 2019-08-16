package com.tencent.hermes.index.test;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

public class TestDaoPai {
	
	  private static FieldType TYPE_INDEXED_AND_STORED = new FieldType();
		static{
			TYPE_INDEXED_AND_STORED.setIndexOptions(IndexOptions.DOCS);
			TYPE_INDEXED_AND_STORED.setStored(false);
			TYPE_INDEXED_AND_STORED.setTokenized(true);
			TYPE_INDEXED_AND_STORED.freeze();
		}
	
	public static void main(String[] args) throws Exception {
		StandardAnalyzer analyzer = new StandardAnalyzer();
		Directory dir = TestUtils.newFsDirectory("D:\\test\\inverted\\index");
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setUseCompoundFile(false);
		IndexWriter w = new IndexWriter(dir, iwc);
		Document doc = new Document();
		doc.add(new Field("content", "love love great", TYPE_INDEXED_AND_STORED));
		doc.add(new Field("title", "love", TYPE_INDEXED_AND_STORED));
		w.addDocument(doc);

		doc.add(new Field("content", "love", TYPE_INDEXED_AND_STORED));
		w.addDocument(doc);
		w.forceMerge(1);
		w.commit();
		
	}

}
