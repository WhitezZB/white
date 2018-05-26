package org.star.white.system;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

/*************
 * 
 * @author KATYNE
 *
 * 2018年5月26日
 */
public class CommandRunner {

	private static final Logger logger = Logger.getLogger(CommandRunner.class);

	private FileOutputStream fosout = null;
	private FileOutputStream foserr = null;

	public CommandRunner(String outputFile, String errorFile) {
		try {
			if (outputFile != null) {
				fosout = new FileOutputStream(new File(outputFile));
			}
			if (errorFile != null) {
				foserr = new FileOutputStream(new File(errorFile));
			}
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (foserr != null) {
			try {
				foserr.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
		if (fosout != null) {
			try {
				fosout.close();
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	public int process(String cmd) {
		cmd = "/bin/sh -c " + cmd;
		String[] cmds = cmd.trim().split("\\s+", 3);
		return process(cmds);
	}

	/**
	 * 执行给定的命令并返回out输出,如果命令执行异常，则返回null
	 * 
	 * @param cmds
	 * @return
	 */
	public String processByReturnOut(String[] cmds) {
		logger.debug(Arrays.asList(cmds));
		int exitVal = 0;
		String error = null;
		String out = null;
		Process proc = null;
		try {
			Runtime rt = Runtime.getRuntime();
			proc = rt.exec(cmds);
			// any error message?
			StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "ERROR", foserr);

			// any output?
			StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT", fosout);

			// kick them off
			//errorGobbler.run();
			//outputGobbler.run();
			//logger.info("before");
			outputGobbler.start();
			errorGobbler.start();
			//logger.info("after start");
			exitVal = proc.waitFor();
			// any error???
			error = errorGobbler.getOutput();
			out = outputGobbler.getOutput();



			// if(error!=null && error.trim().length()>0){
			// exitVal = -100;
			// }
			if (out != null && out.trim().length() > 0) {
				logger.debug("Out: " + out);
			}
			if (error != null && error.trim().length() > 0) {
				logger.error("Err: " + error);
			}
			logger.debug("ExitValue: " + exitVal);
		} catch(Throwable t) {
			t.printStackTrace();
			exitVal = -99;
		} finally {
			try {
				if (foserr != null) {
					foserr.flush();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
			try {
				if (fosout != null) {
					fosout.flush();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
			try {
				if (proc != null) {
					proc.destroy();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			close();
		}
		if (exitVal == 0) {
			return out;
		} else {
			return null;
		}
	}

	/**
	 * exitVal : 255表示already exists异常:eg:No such file or directory<br>
	 * exitVal : -100表示系统命令执行有异常输出, 不代表最终错误<br>
	 * exitVal : -99表示执行命令抛出异常<br>
	 * exitVal : 0表示执行命令正常退出<br>
	 * 注意：调用该方法完毕后必须调用 close()关闭流<br>
	 * 
	 * @param cmds
	 * @return
	 */
	public int process(String[] cmds) {
		logger.debug(Arrays.asList(cmds));
		int exitVal = 0;
		Process proc = null;
		try {

			Runtime rt = Runtime.getRuntime();
			proc = rt.exec(cmds);
			// any error message?
			StreamGobbler errorGobbler = new StreamGobbler(proc.getErrorStream(), "ERROR", foserr);

			// any output?
			StreamGobbler outputGobbler = new StreamGobbler(proc.getInputStream(), "OUTPUT", fosout);

			// kick them off
			errorGobbler.run();
			outputGobbler.run();

			// any error???
			String error = errorGobbler.getOutput();
			String out = outputGobbler.getOutput();

			exitVal = proc.waitFor();

			if (error != null && error.trim().length() > 0) {
				logger.error("Err: " + error);
				// exitVal = -100;
			}
			if (out != null && out.trim().length() > 0) {
				logger.debug("Out: " + out);
			}
			logger.debug("ExitValue: " + exitVal);
		} catch(Throwable t) {
			t.printStackTrace();
			exitVal = -99;
		} finally {
			try {
				if (foserr != null) {
					foserr.flush();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
			try {
				if (fosout != null) {
					fosout.flush();
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
			try {
				if (proc != null) {
					proc.destroy();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			close();
		}
		return exitVal;
	}

	/**
	 * @param cmd
	 * @return
	 */
	public static boolean processCmd(String[] cmd) {
		CommandRunner cr = new CommandRunner(null, null);
		int exitCode = cr.process(cmd);
		cr.close();
		return exitCode == 0;
	}

	/**
	 * 执行给定的命令并返回out输出,如果命令执行异常，则返回null
	 * 
	 * @param cmd
	 * @return
	 */
	public static String processCmdByReturnOut(String[] cmd) {
		CommandRunner cr = new CommandRunner(null, null);
		String out = cr.processByReturnOut(cmd);
		cr.close();
		return out;
	}

	public static void main2(String args[]) {
		if (args.length < 1) {
			System.out.println("USAGE: java GoodWindowsExec <cmd>");
			System.exit(1);
		}

		try {
			String osName = System.getProperty("os.name");
			String[] cmd = new String[3];

			if (osName.equals("Windows NT")) {
				cmd[0] = "cmd.exe";
				cmd[1] = "/C";
				cmd[2] = args[0];
			} else if (osName.equals("Windows 95")) {
				cmd[0] = "command.com";
				cmd[1] = "/C";
				cmd[2] = args[0];
			}
			CommandRunner cr = new CommandRunner(null, null);
			int exitVal = cr.process(cmd);
			cr.close();
			System.out.println("ExitValue: " + exitVal);
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	public static void main(String args[]) {
		if (args.length < 1) {
			System.out.println("USAGE java CommandRunner <cmds...>");
			System.exit(1);
		}
		try {
			CommandRunner cr = new CommandRunner(null, null);
			int exitVal = cr.process(args);
			System.out.println("ExitValue: " + exitVal);
			String out = cr.processByReturnOut(args);
			System.out.println(out);
			cr.close();

		} catch(Throwable t) {
			t.printStackTrace();
		}
	}
}