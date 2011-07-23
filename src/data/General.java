package data;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class General {
	/* **************
	 * Helper Methods
	 * **************/

	/**
	 * Get a BufferedWriter for a given input file path.
	 * @param inpath
	 * @return
	 * @throws FileNotFoundException
	 */
	public static BufferedReader getBufferedReader(String inpath) throws FileNotFoundException {
		// create read input buffer
		FileInputStream inStream = new FileInputStream(inpath);
		DataInputStream in = new DataInputStream(inStream);
		return new BufferedReader(new InputStreamReader(in));
	}
	
	/**
	 * Get a BufferedReader for a given output file path.
	 * @param outpath
	 * @return
	 * @throws FileNotFoundException
	 */
	public static BufferedWriter getBufferedWriter(String outpath) throws FileNotFoundException {
		// create write output buffer
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		return new BufferedWriter(new OutputStreamWriter(out));
	};
	
	/**
	 * Get now in the given format.
	 */
	public static String getNow(String format) {
		return (new SimpleDateFormat(format)).format(Calendar.getInstance().getTime());
	}
	
	
	/**
	 * Get now in "yyyy-MM-dd HH:mm:ss" format.
	 */
	public static String getNow() {
		return (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime());
	}
}
