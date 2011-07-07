package data;

import java.io.*;

public class DataManipulation {

	/**
	 * Create a random dataset with given size of transactors and number of transactions, and create a
	 * corresponding ARFF file with "transactor" and the list of transactors as attributes. The data is represented
	 * as an ARFF sparse vector.
	 * The transactor names will be positive integers.
	 * @param numOfTransactors
	 * 					The number of transactors to be generated.
	 * @param numOfTransactions
	 * 					The number of transactions to be generated.
	 * @param outpath
	 * 					The path to the desired output filename (a ".arff" file).
	 * @throws FileNotFoundException 
	 */
	public static void genRandArff(int numOfTransactors, int numOfTransactions, String outpath) throws FileNotFoundException {
		// create output write buffer
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		// generate list of transactors
		
	}
}
