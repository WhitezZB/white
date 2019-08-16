package org.white.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/*************
 * 
 * @author kaynewu
 *
 * 2019年7月22日
 */
public class PSchema {
	
	
	/*****
	 * 
	 * @param schemaStr
	 * @return
	 */
	public static  MessageType parseParquetSchema(String schemaStr) {
		return MessageTypeParser.parseMessageType(schemaStr);
	}

}
