package org.star.white.hadoop;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.star.white.WhiteConfigConstant;
import org.star.white.conf.GlobalInfo;
import org.star.white.system.CommandRunner;

/**********
 * 
 * @author KATYNE
 *
 * 2018年5月26日
 */
public class HadoopUtil {
	public static final Logger logger = Logger.getLogger(HadoopUtil.class);

	public static String HADOOP_HOME;
	public static String HADOOP_CONF;
	private static Configuration remoteConf = null;
	private static FileSystem HADOOP_FILESYSTEM;
	private static final Properties properties = GlobalInfo.getProperties();

	static {
		HADOOP_HOME = properties.getProperty("hadoop.home");
		HADOOP_CONF = properties.getProperty("hadoop.conf.dir");
//		remoteConf = HadoopUtil.grabConfiguration(HadoopUtil.HADOOP_CONF, remoteConf);
		try {
			String white_home = System.getProperty(WhiteConfigConstant.WHITE_HOME);
			if(white_home == null){
				white_home = ".";
			}
			System.out.println("white_home:" + white_home);
			String log4jpath = System.getProperty("log4j.properties.path");
			logger.info("System.getProperty(\"log4j.properties.path\") is " + log4jpath);
			if (log4jpath != null) {
				File log4jFile = new File(white_home, log4jpath);
				System.out.println(log4jFile.getAbsolutePath());
				Properties log4jProperties = new Properties();
				log4jProperties.load(new FileReader(log4jFile));
				PropertyConfigurator.configure(log4jProperties);
			}
			remoteConf = grabConfiguration(HADOOP_CONF, new Configuration());
		} catch(Exception e) {
			logger.error("Don't find log4j.properties", e);
			e.printStackTrace();
				try {
					String log4jpath = properties.getProperty("docker.image.file.path") + "/conf/log4j.properties";
					if (log4jpath != null) {
						File log4jFile = new File(log4jpath);
						System.out.println(log4jFile.getAbsolutePath());
						Properties log4jProperties = new Properties();
						log4jProperties.load(new FileReader(log4jFile));
						PropertyConfigurator.configure(log4jProperties);
					}
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
			}
		
	}
	
	
	public static FileSystem getFs(Configuration conf,Path path) throws IOException {
		FileSystem rtn = null;
		if(path == null){
			rtn = FileSystem.get(conf);
		}else{
			rtn = path.getFileSystem(conf);
		}
		rtn.setConf(conf);
		return rtn;
	}
	
	public static Object confLock = new Object();
	public static Configuration getRemoteConf(){
		synchronized (confLock) {
			if(remoteConf == null){
				remoteConf = grabConfiguration(HADOOP_CONF, new Configuration());
			}
			return remoteConf;
		}
	}
	
	public static void upadateConf() {
		// TODO Auto-generated method stub
		synchronized (confLock) {
			remoteConf = grabConfiguration(HADOOP_CONF, new Configuration());
		}
	}
	
	
	public static Configuration copyToLocalConf(){
			Configuration rtn = grabConfiguration(HADOOP_CONF, new Configuration());
			return rtn;
	}
	
	public static String getOutFileName(int partition, String prefix) {
		
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);
		StringBuilder result = new StringBuilder();
		result.append(prefix);
		result.append("-");
		result.append(nf.format(partition));
		return result.toString();
	}
	

	/**
	 * 获得higo.properties中higo.hadoop.home.conf属性对应的FileSystem对象
	 * 
	 * @return
	 * @throws IOException
	 */
	public static FileSystem getFileSystem() throws IOException {
		if (HADOOP_FILESYSTEM == null) {
			Configuration conf = grabConfiguration(HADOOP_CONF, new Configuration());
			HADOOP_FILESYSTEM = FileSystem.get(conf);
		}
		return HADOOP_FILESYSTEM;
	}
	
	public static FileSystem getFileSystem(Path path) {
		FileSystem fs = null;
		try {
			Configuration conf = remoteConf;//grabConfiguration(HADOOP_CONF, new Configuration());
			if (path != null) {
				fs = path.getFileSystem(conf);
			} else {
				fs = FileSystem.get(conf);
			}
			return fs;
		} catch(IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Configuration grabConfiguration(String hadoopConfDir, Configuration conf) {
		boolean oldVersionHadoop = new File(hadoopConfDir, "hadoop-default.xml").exists() || new File(hadoopConfDir, "hadoop-site.xml").exists(); 
		String[] files = null;
		if(oldVersionHadoop){
			files = new String[]{"hadoop-default.xml","hadoop-site.xml","yarn-site.xml"};
		}else{
			files = new String[]{"hdfs-site.xml","httpfs-site.xml","mapred-site.xml",
					"core-site.xml","yarn-site.xml","fair-scheduler.xml","hadoop-policy.xml","capacity-scheduler.xml"};
			
		}
		for(String xml : files){
			try {
				conf.addResource(new Path(hadoopConfDir, xml));
			} catch (Exception e) {
				// TODO: handle exception
			}				
		}
		return conf;
	}
	

	/**
	 * @param localPath
	 * @param destFs
	 * @return
	 */
	public static boolean copyFileFromLocal(String localPath, String destPath, Configuration destConf) {
		try {
			FileSystem destFs = getFs(destConf, new Path(destPath));
			destFs.copyFromLocalFile(new Path(localPath), new Path(destPath));
		} catch(IOException ioe) {
			ioe.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 拷贝hdfs目录到本地
	 * @param localPath
	 * @param destFs
	 * @return
	 */
	public static boolean copyDirToLocal(Path remotePath, Path localPath) {
		try {
			File lf = new File(localPath.toString());
			if(lf.exists()){
				logger.error(localPath+" had existed");
				return false;
			}
			lf.mkdirs();
			FileSystem fs = remotePath.getFileSystem(getRemoteConf());
			FileStatus[] fileStatuss = fs.listStatus(remotePath);
			for(FileStatus fileStatus : fileStatuss){
				Path temp = fileStatus.getPath();
				if(fs.isFile(temp)){
					int retry = 5;
					while(retry > 0){
						try{
							fs.copyToLocalFile(temp, new Path(localPath, temp.getName()));
							break;
						}catch(Throwable t){
							//logger.error("Source Path:" + temp.toString() + "Dest Path:" + (new Path(localPath, temp.getName())).toString());
							logger.error("Source Path:" + temp.toString() + "Dest Path:" + (new Path(localPath, temp.getName())).toString());
							t.printStackTrace();
							retry--;
						}
					}
				} else {
					copyDirToLocal(remotePath, localPath);
				}
			}
		} catch(IOException ioe) {
			ioe.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 
	 * @param remoteFilePath
	 * @param localFilePath
	 * @param remoteConf
	 * @return
	 */
	public static boolean copyFileToLocal(Path remoteFilePath, Path localFilePath) {
		try {
			FileSystem fs = remoteFilePath.getFileSystem(getRemoteConf());
			File lf = new File(localFilePath.toString());
			if(lf.exists()){
				logger.info(localFilePath+" had existed");
			} else {
				lf.getParentFile().mkdirs();
				int retry = 5;
				while(retry > 0){
					try{
						fs.copyToLocalFile(remoteFilePath, localFilePath);
						return true;
					}catch(Throwable t){
						System.out.println("Source Path:" + remoteFilePath.toString() + "Dest Path:" + localFilePath.toString());
						t.printStackTrace();
						retry--;
					}
				}
			}
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 
	 * @param remoteFilePath
	 * @param localFilePath
	 * @param remoteConf
	 * @return
	 */
	public static boolean copyFileToLocal(Path remoteFilePath, Path localFilePath, Configuration remoteConf) {
		try {
			FileSystem fs = remoteFilePath.getFileSystem(remoteConf);
			File lf = new File(localFilePath.toString());
			if(lf.exists()){
				logger.info(localFilePath+" had existed");
			} else {
				lf.getParentFile().mkdirs();
				int retry = 5;
				while(retry > 0){
					try{
						fs.copyToLocalFile(remoteFilePath, localFilePath);
						return true;
					}catch(Throwable t){
						System.out.println("Source Path:" + remoteFilePath.toString() + "Dest Path:" + localFilePath.toString());
						t.printStackTrace();
						retry--;
					}
				}
			}
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 拷贝hdfs上srcPath到本地destPath路径
	 * 
	 * @param fileOnHdfsPath
	 * @param absolutePath
	 * @return
	 */
	public static boolean copyFile(String srcPath, String destPath) {
		String downloadFromHdfs = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -get " + srcPath + " " + destPath;
		boolean success = CommandRunner.processCmd((downloadFromHdfs).split(" "));
		return success;
	}

	/**
	 * 上传到hdfs的destPath路径下并且不更改srcPath对应的文件名
	 * 
	 * @param srcPath
	 *            源文件绝对路径
	 * @param destPath
	 *            目标父目录
	 * @return
	 */
	public static boolean uploadFile(String srcPath, String destPath) {
		String filename = new File(srcPath).getName();
		String mkdirCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -mkdir " + destPath;
		boolean mkdirCmdSuccess = CommandRunner.processCmd((mkdirCmd).split(" "));
		System.out.println("uploadFile exec " + mkdirCmd + " " + mkdirCmdSuccess);
		// 删除旧的docids文件
		String deleteFromHdfsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -rmr " + new File(destPath, filename).getAbsolutePath();
		boolean deleteSuccess = CommandRunner.processCmd((deleteFromHdfsCmd).split(" "));
		System.out.println("uploadFile exec " + deleteFromHdfsCmd + " " + deleteSuccess);
		String uploadToHdfsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -put " + srcPath + " " + destPath;
		boolean success = CommandRunner.processCmd((uploadToHdfsCmd).split(" "));
		System.out.println("uploadFile exec " + uploadToHdfsCmd + " " + success);
		return success;
	}

	/**
	 * 上传到hdfs的destPath的父路径下并且重命名srcPath对应的文件名为destPath对应的文件名
	 * 
	 * @param srcPath
	 * @param destPath
	 * @return
	 */
	public static boolean uploadAndRenameFile(String srcPath, String destPath) {
		String destParentPath = new File(destPath).getParent();
		String newName = new File(destPath).getName();
		String mkdirCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -mkdir " + destParentPath;
		boolean mkdirCmdSuccess = CommandRunner.processCmd((mkdirCmd).split(" "));
		System.out.println("uploadAndRenameFile exec " + mkdirCmd + " " + mkdirCmdSuccess);
		// 删除旧的docids文件
		String deleteFromHdfsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -rmr " + new File(destParentPath, newName).getAbsolutePath();
		boolean deleteSuccess = CommandRunner.processCmd((deleteFromHdfsCmd).split(" "));
		System.out.println("uploadAndRenameFile exec " + deleteFromHdfsCmd + " " + deleteSuccess);
		String uploadToHdfsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -put " + srcPath + " " + destPath;
		boolean success = CommandRunner.processCmd((uploadToHdfsCmd).split(" "));
		System.out.println("uploadAndRenameFile exec " + uploadToHdfsCmd + " " + success);
		return success;
	}

	/**
	 * 判断给定的路径或者文件是否存在
	 * 
	 * @param fileOnHdfsPath
	 * @return
	 */
	public static boolean exist(String fileOnHdfsPath) {
		String lsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -ls " + fileOnHdfsPath;
		return CommandRunner.processCmd(lsCmd.split(" "));
	}

	/**
	 * 获得input对应文件的大小, 单位为byte
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static long getSizeWithByte(String input) throws IOException {
		FileStatus fileStatus = HadoopUtil.getFileSystem().getFileStatus(new Path(input));
		if (fileStatus != null) {
			return fileStatus.getLen();
		} else {
			return 0;
		}
	}
	
	/**
	 * 
	 * @param size
	 * @param inputPath
	 * @throws IOException
	 */
	public static void getSizeWithByte(AtomicLong size, Path inputPath) throws IOException {
		if(inputPath != null && inputPath.getFileSystem(getRemoteConf()).exists(inputPath)){
			FileSystem fs = inputPath.getFileSystem(getRemoteConf());
			if(fs.isDirectory(inputPath)){ // dir
				FileStatus[] fileStatus = fs.listStatus(inputPath);
				for(FileStatus status : fileStatus){
					Path temp = status.getPath();
					if(fs.isFile(temp)){
						size.addAndGet(fs.getLength(temp));
					} else {
						getSizeWithByte(size, temp);
					}
				}
			} else { // file 
				size.addAndGet(fs.getLength(inputPath));
			}
		} else {
			throw new IOException("inputPath:"+inputPath.toString()+" unvalid");
		}
	}
	
	public static AtomicLong getChirldSize(AtomicLong size, Path inputPath) throws IOException {
		if(inputPath != null && inputPath.getFileSystem(getRemoteConf()).exists(inputPath)){
			FileSystem fs = inputPath.getFileSystem(getRemoteConf());
			if(fs.isDirectory(inputPath)){ // dir
				FileStatus[] fileStatus = fs.listStatus(inputPath);
				for(FileStatus status : fileStatus){
					Path temp = status.getPath();
					if(fs.isFile(temp)){
						size.addAndGet(1);
					} else {
						getSizeWithByte(size, temp);
					}
				}
			} else { // file 
				size.addAndGet(1);
			}
		} else {
			throw new IOException("inputPath:"+inputPath.toString()+" unvalid");
		}
		return size;
	}
	
	
	public static AtomicLong getImportZkChirldSize(AtomicLong size, Path inputPath) throws IOException {
		if(inputPath != null && inputPath.getFileSystem(getRemoteConf()).exists(inputPath)){
			FileSystem fs = inputPath.getFileSystem(getRemoteConf());
			if(fs.isDirectory(inputPath)){ // dir
				FileStatus[] fileStatus = fs.listStatus(inputPath);
				for(FileStatus status : fileStatus){
					Path temp = status.getPath();
					if(fs.isFile(temp)){
						size.addAndGet(1);
						if(size.get() > 1001){
							return size;
						}
					} else {
						return getImportZkChirldSize(size, temp);
					}
				}
			} else { // file 
				size.addAndGet(1);
				if(size.get() > 1001){
					return size;
				}
			}
		} else {
			throw new IOException("inputPath:"+inputPath.toString()+" unvalid");
		}
		return size;
	}
	
	/**
	 * 传入从bin目录开始的命令
	 * 
	 * @param cmds
	 * @return
	 */
	public static String processHadoopCmd(String cmds) {
		String cmd = HadoopUtil.HADOOP_HOME + "/" + cmds;
		return CommandRunner.processCmdByReturnOut(cmd.split(" "));
	}

	/**
	 * 返回bitspath目录下的文件列表
	 * 
	 * @param bitsPath
	 * @return
	 */
	public static List<String> listFile(String bitsPath) {
		String lsCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -ls " + bitsPath;
		String returnVal = CommandRunner.processCmdByReturnOut(lsCmd.split(" "));
		ArrayList<String> list = new ArrayList<String>();
		if (returnVal != null) {
			String[] rvs = returnVal.split("\n");
			for (String rv : rvs) {
				if (rv.contains(bitsPath)) {
					list.add(rv);
				}
			}
		}
		return list;
	}

	/**
	 * @param absolutePath
	 * @return
	 */
	public static boolean delete(String absolutePath, boolean deleteSubFile) {
		String tmp = absolutePath;
		if (deleteSubFile) {
			tmp = new File(tmp, "*").getAbsolutePath();
		}
		String rmrCmd = HadoopUtil.HADOOP_HOME + "/bin/hadoop fs -rmr " + tmp;
		return CommandRunner.processCmd(rmrCmd.split(" "));
	}
	
	/**
	 * 当partition为1时, 获得的结果为part-r-00001
	 * 
	 * @param partition
	 * @return
	 */
	public static String formatPartNo(int partition) {
		return formatOutFileName(partition, "part-r");
	}
	
	public static Path getPath(String base, String ...suffixs){
		Path path = new Path(base);
		for(String suffix : suffixs){
			path = new Path(path,suffix);
		}
		return path;
	}

	/**
	 * @param partition
	 * @param prefix
	 * @return
	 */
	private static String formatOutFileName(int partition, String prefix) {
		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(5);
		nf.setGroupingUsed(false);
		StringBuilder result = new StringBuilder();
		result.append(prefix);
		result.append("-");
		result.append(nf.format(partition));
		return result.toString();
	}
	
	
	/**
	 * 
	 * @param hadoopConf
	 * @param src
	 * @param destFile
	 * @return
	 * @throws IOException
	 */
	public static String upload(String src, String destFile) throws IOException {
		try {
			System.out.println("upload datafile from " + src + " to " + destFile);
			Path srcPath = new Path(src);
			Path destPath = new Path(destFile);
			FileSystem fs = destPath.getFileSystem(getRemoteConf());

			if (!fs.exists(destPath.getParent())) {
				fs.mkdirs(destPath.getParent());
			}
			if(!fs.exists(destPath)){
				fs.copyFromLocalFile(false, true, srcPath, destPath);
				return destPath.toString();
			} else {
				System.out.println(destPath+" existed");
			}
		} catch(Throwable t) {
			t.printStackTrace();
		}
		return null;
	}
	
	/**
	 * new instance for File "base/fileNames"
	 * 
	 * @param base
	 * @param fileNames
	 * @return
	 */
	public static Path joinPath(String base, String... fileNames) {
		if (base == null) {
			throw new NullPointerException("base must be not null");
		}
		Path baseFile = new Path(base);
		if (fileNames != null && fileNames.length > 0) {
			for (String filename : fileNames) {
				baseFile = new Path(baseFile, filename);
			}
		}
		return baseFile;
	}
	
	
	public static int getPartNumbers(Configuration conf, Path path){
		try {
			FileSystem fs = path.getFileSystem(conf);
			FileStatus[] list = fs.listStatus(path, new PathFilter() {				
				@Override
				public boolean accept(Path path) {
					// TODO Auto-generated method stub
					if(Pattern.matches("part-[0-9][0-9][0-9][0-9][0-9]",path.getName())){
						return true;
					}
					return false;
				}
			});
			return list.length;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return 0;
	}
	
	
	public static int parserPartNo(String path){
		try {
			Path p = new Path(path);
			String name = p.getName();
			String[] sArray = name.split("-");
			return Integer.parseInt(sArray[1]);
		} catch (Exception e) {
			// TODO: handle exception
		}		
		return -1;
	}
	

	/**
	 * 传入从bin目录开始的命令
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
	}




}
