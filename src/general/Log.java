package general;

import java.io.*;

import data.General;

/**
 * Logging for debug purposes
 */
public class Log {

	private static BufferedWriter bw = null;
	private static final String logfile_dir = "D:\\data\\workspace\\java\\wharton.reidentification\\logs";
	private static final String logfile_date_format = "yyyy-MM-dd_HH-mm-ss";
	private static final String log_msg_prefix = "[LOG] ";
	
	public enum STDTypeEnum {
		STDOUT,
		STDERR;
	}
	
	public static String now() {
		return General.getNow("HH:mm:ss")+": ";
	}
	
	public static void log(String msg, STDTypeEnum std) {
		// initialize log file if necessary
		if (bw == null) {
			try {
				String name = logfile_dir+"\\"+"log_"+General.getNow(logfile_date_format)+".txt";
				bw = General.getBufferedWriter(name);
				bw.write(log_msg_prefix+now()+"Log file opened: "+name+"\n");
				bw.flush();
				System.out.println(log_msg_prefix+now()+"Log file opened: "+name);
			} catch (IOException e) {
				System.err.println(log_msg_prefix+now()+"IO Exception thrown when tried to open a log file.");
				e.printStackTrace();
			}
		}
		
		// write to log file
		try {
			bw.write(log_msg_prefix+now()+msg+"\n");
			bw.flush();
		} catch (IOException e) {
			System.err.println(log_msg_prefix+now()+"IO Exception thrown when tried to write message:\n"+
					"'"+msg+"'\n"+
					"to log file.");
			e.printStackTrace();
		}
		
		// write to selected std
		if (std == STDTypeEnum.STDOUT) {
			System.out.println(log_msg_prefix+now()+msg);
		} else if (std == STDTypeEnum.STDERR) {
			System.err.println(log_msg_prefix+now()+msg);
		} else {
			System.err.println(log_msg_prefix+now()+"Unknown STD given.");
		}
	}
	
	/**
	 * close log file
	 */
	public static void closeLog() {
		try {
			bw.flush();
			bw.close();
		} catch (IOException e) {
			System.err.println(log_msg_prefix+now()+"IO Exception thrown when tried to close log file.");
			e.printStackTrace();
		}
	}
}
