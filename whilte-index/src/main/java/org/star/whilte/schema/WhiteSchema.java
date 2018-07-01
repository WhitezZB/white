package org.star.whilte.schema;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.star.white.conf.GlobalInfo;
import org.xml.sax.InputSource;


/*************
 * 
 * @author KATYNE
 *
 * 2018年6月10日
 */
public class WhiteSchema extends org.apache.solr.schema.IndexSchema{
	
	private static Logger logger = Logger.getLogger(WhiteSchema.class);
	static {
		System.setProperty("solr.allow.unsafe.resourceloading", "true");
	}
	
	public static WhiteSchema LOCAL(Path path) {
		try {
			FileSystem fs =FileSystem.getLocal(new Configuration());
			WhiteSchema rtnrrtn=new WhiteSchema(new SolrConfig( GlobalInfo.getCurrentPath() + "/conf/solrconfig.xml") ,path.toString(), new InputSource(fs.open(path)));
			return rtnrrtn;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			logger.error("schema read error:", e);
		}
		return null;

	}
	
	protected WhiteSchema(SolrConfig solrConfig, SolrResourceLoader loader) {
		super(solrConfig, loader);
	}

	public WhiteSchema(SolrConfig solrConfig, String string, InputSource inputSource) {
		// TODO Auto-generated constructor stub
		super(solrConfig, string, inputSource);
	}
	
	public static void main(String[] args) throws IllegalArgumentException, Exception {
		WhiteSchema schema = WhiteSchema.LOCAL(new Path("D:\\test\\lucene\\schema.xml"));
		schema.getIndexAnalyzer();
		schema.getFields();
	}

}
