package org.white.parquet.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/*********
 * Parquet meger
 * @author kaynewu
 *
 * 2019年7月24日
 */
public interface PMerge {

	public boolean merge(Configuration conf, Path outputFile, Path...inputFiles) throws Exception;
}
