package org.star.white.white_utils;


import org.star.white.conf.ConfigRead;
import org.star.white.conf.UniqConfig;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
       ConfigRead.init();
       while(true) {
    	   System.out.println(UniqConfig.toJson());
    	   Thread.sleep(15000);
       }
    }
}
