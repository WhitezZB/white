package org.star.whie.common.string;

import java.util.concurrent.LinkedBlockingQueue;


/**********
 * 字符拼接工具类
 * @author kaynewu
 *
 */
public class KStringBuilder {
	private static int LENGTH = 128;
	private static LinkedBlockingQueue<StringBuilder> stringBuilders = new LinkedBlockingQueue<StringBuilder>(LENGTH);
	static {
		for (int i = 0; i < LENGTH; i++) {
			StringBuilder builder = new StringBuilder();
			stringBuilders.add(builder);
		}
	}
	
	public static String builder(Object ...strs) {
		StringBuilder builder;
		try {
			builder = stringBuilders.poll();
			if(builder == null) {
				builder = new  StringBuilder();
			}
		} catch (Exception e) {
			builder = new  StringBuilder();
		}
		for (int i = 0; i < strs.length; i++) {
			Object str = strs[i];
			builder.append(str);
		}
		String rtnVal = builder.toString();
		builder.setLength(0);
		stringBuilders.add(builder);
		return rtnVal;		
	}
}
