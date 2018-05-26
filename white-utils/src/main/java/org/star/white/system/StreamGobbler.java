package org.star.white.system;

import java.io.*;

public class StreamGobbler extends Thread {
	private InputStream is;
	private String type;

	public String getType() {
		return type;
	}

	private OutputStream os;
	private StringBuilder sb;

	public StreamGobbler(InputStream is, String type) {
		this(is, type, null);
	}

	public StreamGobbler(InputStream is, String type, OutputStream redirect) {
		this.is = is;
		this.type = type;
		this.os = redirect;
		sb = new StringBuilder();
	}

	public String getOutput() {
		return sb.toString();
	}

	public void run() {
		BufferedReader br = null;
		PrintWriter pw = null;
		try {
			if (os != null)
				pw = new PrintWriter(os);

			br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (pw != null) {
					pw.println(line);
				}
				sb.append(line + "\n");
			}
			if (pw != null) {
				pw.flush();
			}
		} catch(IOException ioe) {
			//ioe.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch(IOException e) {
					e.printStackTrace();
				}
			}
			if (pw != null) {
				pw.close();
			}
		}
	}
}
