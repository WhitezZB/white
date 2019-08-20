package com.tencent.hermes.store;

import java.util.Arrays;

import com.tencent.hermes.store.utils.BytesRefUtils;
import com.tencent.hermes.store.utils.BytesRef;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
