package com.tencent.sdk.index.data;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.tencent.sdk.index.IndexDataCenter;


/*****
 * 
 * @author kaynewu
 *
 * 2019年7月30日
 */
public class ProtoDataParser extends DataParser<Message, ByteOrigin>{
	private static final Logger logger = Logger.getLogger(ProtoDataParser.class);
	
	private static IndexDataCenter<Message> center = new IndexDataCenter<>(DataType.PROTO);
	
	
	private Class<? extends Message> protoMessage;
	private Method method;
	public ProtoDataParser(Class<? extends Message> protoMessage) throws Exception {
		this.protoMessage = protoMessage;
		method = this.protoMessage.getMethod("parseFrom"); 
	}

	@Override
	public IndexDocument<Message> parser(ByteOrigin org) {
		try {
			if(org != null) {
				Message message = this.parseData(org.getData());
				IndexDocument<Message> doc = new IndexDocument<Message>(org.tablename,
						this.getThedate(), this.getPartNo(), message, org.getSeqId());
				return doc;
			}
		} catch (Exception e) {
			 logger.error("parser error",e);
		}
		return null;
	}
	
	@Override
	public int getPartNo() {
		return 0;
	}

	private Message parseData(byte[] data) {
		try {
			Object obj = this.method.invoke(null, data);
			return (Message)obj;
		} catch (Exception e) {
			 logger.error("parserData error",e);
		}
		return null;
	}

	@Override
	public void sendDoc(IndexDocument<Message> doc) {
		center.push(doc);
	}

}
