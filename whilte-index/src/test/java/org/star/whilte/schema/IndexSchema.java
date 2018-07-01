package org.star.whilte.schema;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


/*************
 * 
 * @author KATYNE
 *
 * 2018年6月10日
 */
public class IndexSchema extends org.apache.solr.schema.IndexSchema{
	
	public static IndexSchema LOCAL(Path path) throws Exception {
		FileSystem fs =FileSystem.getLocal(new Configuration());
		IndexSchema rtnrrtn=new IndexSchema(new SolrConfig() ,path.toString(), new InputSource(fs.open(path)));
		return rtnrrtn;

	}
	
	protected IndexSchema(SolrConfig solrConfig, SolrResourceLoader loader) {
		super(solrConfig, loader);
	}

	public IndexSchema(SolrConfig solrConfig, String string, InputSource inputSource) {
		// TODO Auto-generated constructor stub
		super(solrConfig, string, inputSource);
	}

}
