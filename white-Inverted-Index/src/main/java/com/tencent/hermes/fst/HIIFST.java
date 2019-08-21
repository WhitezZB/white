package com.tencent.hermes.fst;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import com.tencent.hermes.inverted.field.Field;

/***************
 * Hermes inverted index FST
 * @author kaynewu
 *
 * 2019年8月21日
 */
public class HIIFST {
	private Field field;
	private Builder<Long> builder; 
	private IntsRefBuilder scratchInts = new IntsRefBuilder();
	
	public HIIFST(Field field) {
		this.field = field;
		this.builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1,
				PositiveIntOutputs.getSingleton());
	}
	
	public void add(BytesRef ref,long value) throws IOException {
		builder.add(Util.toIntsRef(ref, scratchInts), value);
	}
	
	

}
