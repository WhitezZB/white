package org.star.whie.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/********
 * 对象处理通用工具
 * @author kaynewu
 *
 */
public class ObjectUtils {
	public static boolean persistenceObject(String filePath, Object obj) {
		File destFile = new File(filePath);
		if(!destFile.getParentFile().exists()){
			destFile.getParentFile().mkdirs();
		}
		
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(destFile);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(obj);
			return true;
		} catch(Exception e) {
			if(destFile.exists()){
				destFile.delete();
			}
			e.printStackTrace();
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param file
	 * @return
	 */
	public static Object deserialize(String filePath){
		File file = new File(filePath);
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(file);
			ObjectInputStream ois = new ObjectInputStream(fis);
			Object obj = ois.readObject();
			return obj;
		} catch(Exception e) {
			e.printStackTrace();
		} finally{
			if(fis != null){
				try {
					fis.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
}
