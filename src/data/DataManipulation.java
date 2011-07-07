package data;

import java.io.*;
import java.text.*;
import java.util.*;

public class DataManipulation {

	public static final int MAX_NUM_OF_TRANSACTIONS_PER_EDGE = 300;
	
	public static void main(String[] args) {
		try {
			genRandArff(10,100,"D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff\\rand_data\\10_100.arff");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/**
	 * Create a random dataset with given size of transactors and number of transactions, and create a
	 * corresponding ARFF file with "transactor" and the list of transactors as attributes. The data is represented
	 * as an ARFF sparse vector.
	 * The transactor names will be positive integers.
	 * @param numOfTransactors
	 * 					The number of transactors to be generated.
	 * @param numOfEdges
	 * 					The number of edges to be generated. Should be <= numOfTransactors^2
	 * @param outpath
	 * 					The path to the desired output filename (a ".arff" file).
	 * @throws FileNotFoundException 
	 */
	public static void genRandArff(int numOfTransactors, int numOfEdges, String outpath) throws IOException {
		// create output write buffer
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		int i;
		
		// create and write header
		String header = "% Random Generated Dataset - Generated on "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())+"\n"+
				"% Number of transactors: "+numOfTransactors+"\n"+
				"% Number of transactions: "+numOfEdges+"\n"+
				"\n"+
				"@RELATION Random_Generated_Dataset-"+numOfTransactors+"_transactors-"+numOfEdges+"_transactions\n"+
				"\n"+
				"% class attribute - transactor"+
				"@ATTRIBUTE transactor INTEGER % -- to be discritize to nominal\n"+
				"\n"+
				"% list of transactor attributes to hold number of transactions per edge\n";
		bw.write(header);
		bw.flush();
		
		// create and write list of attributes - one per transactor
		for (i=0; i < numOfTransactors; i++) {
			bw.write("@ATTRIBUTE "+i+" INTEGER\n");
			if (i % 100 == 0) bw.flush();
		}
		bw.write("\n");
		bw.flush();
		
		/* randomly create data */
		
		// check / set number of edges
		int minNumOfEdges = (int) (numOfTransactors + 0.25*numOfTransactors);
		int maxNumOfEdges = numOfTransactors*numOfTransactors;
		
		if (numOfEdges < minNumOfEdges || numOfEdges > maxNumOfEdges)
			numOfEdges = (int) (minNumOfEdges + Math.ceil((maxNumOfEdges - minNumOfEdges)*Math.random()));
		
		// create list of indices in the adjacency matrix to be non zero
		int[] indices = new int[numOfEdges];
		Random r = new Random();
		for (i=0; i<numOfEdges; i++) {
			indices[i] = r.nextInt(maxNumOfEdges); // size of the adjacency matrix
		}
		
		// write the data in a weka sparse vector format
		int j;
		int k = 0;
		for (i=0; i<numOfTransactors; i++) {
			bw.write("{0 "+i);
			for (j=0; j<numOfTransactors; j++) {
				if (i*numOfTransactors+j != indices[k]) continue;
				bw.write(", "+indices[k++]+" "+(r.nextInt(MAX_NUM_OF_TRANSACTIONS_PER_EDGE)+1));
			}
			bw.write("}\n");
			if (i % 100 == 0) bw.flush();
		}
		bw.flush();
		out.close();
	}
	
}
