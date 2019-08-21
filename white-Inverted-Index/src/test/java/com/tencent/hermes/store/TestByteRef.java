package com.tencent.hermes.store;


import org.apache.lucene.util.BytesRef;

import com.tencent.hermes.store.utils.BytesRefUtils;

import junit.framework.TestCase;

public class TestByteRef extends TestCase{
	
 

    /**
     * Rigourous Test :-)
     */
    public void testByteRef()
    {
        String a = "a^a";
        BytesRef ref = new BytesRef(a);
        System.out.println(ref.toString());
        System.out.println(BytesRefUtils.byteRefToReadable(ref));
        
    }
	

}
