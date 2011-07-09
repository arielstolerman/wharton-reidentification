package data;

import java.io.*;
import java.text.*;
import java.util.*;
import weka.core.*;

public class DataManipulation {

	public static final int MAX_NUM_OF_TRANSACTIONS_PER_EDGE = 1000;
	public static final double MIN_NUM_OF_EDGES_PER_TRANSACTOR = 50;
	public static final double MAX_NUM_OF_EDGES_PER_TRANSACTOR = 300;
	
	/**
	 * Create a random dataset with given size of transactors and number of transactions, and create a
	 * corresponding ARFF file with "transactor" and the list of transactors as attributes. The data is represented
	 * as an ARFF sparse vector.
	 * The transactor names will be positive integers.
	 * @param numOfTransactors
	 * 					The number of transactors to be generated.
	 * @param outpath
	 * 					The path to the desired output filename (a ".arff" file).
	 * @throws FileNotFoundException 
	 */
	public static void genRandArff(int numOfTransactors, String outpath) throws IOException {
		// create output write buffer
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		int i;
		
		// create and write header
		bw.write("% Random Generated Dataset - Generated on "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(Calendar.getInstance().getTime())+"\n"+
				"% Number of transactors: "+numOfTransactors+"\n"+
				"\n"+
				"@RELATION Random_Generated_Dataset-"+numOfTransactors+"_transactors\n"+
				"\n"+
				"% class attribute - transactor\n"+
				"@ATTRIBUTE transactor {0");
		bw.flush();
		
		for (i=1; i<numOfTransactors; i++) {
			bw.write(","+i);
			if (i % 100 == 0) bw.flush();
		}
		
		bw.write("}\n\n"+
				"% list of transactor attributes to hold number of transactions per edge\n");
		bw.flush();
		
		// create and write list of attributes - one per transactor
		for (i=0; i < numOfTransactors; i++) {
			bw.write("@ATTRIBUTE "+i+" INTEGER\n");
			if (i % 100 == 0) bw.flush();
		}
		bw.write("\n");
		bw.flush();
		
		/* randomly create data */		
		
		// write the data in a weka sparse vector format
		bw.write("@DATA\n");
		Random r = new Random();
		int j;
		List<Integer> indices;
		int numOfIndices;
		int numOfEdges = 0;
		
		for (i=0; i<numOfTransactors; i++) {
			bw.write("{0 "+i);
			
			// generate indices to have non zero values
			indices = new ArrayList<Integer>();
			numOfIndices = (int) Math.ceil(MIN_NUM_OF_EDGES_PER_TRANSACTOR + Math.random() * (MAX_NUM_OF_EDGES_PER_TRANSACTOR - MIN_NUM_OF_EDGES_PER_TRANSACTOR));
			numOfEdges += numOfIndices;
			for (j=0; j<numOfIndices; j++) {
				indices.add(r.nextInt(numOfTransactors));
			}
			Collections.sort(indices);
			
			for (j=0; j<numOfTransactors; j++) {
				// generate indices to be non zero
				if (indices.contains(j))
					bw.write(", "+(j+1)+" "+(r.nextInt(MAX_NUM_OF_TRANSACTIONS_PER_EDGE)+1));
			}
			bw.write("}\n");
			if (i % 100 == 0) bw.flush();
		}
		bw.flush();
		out.close();
		
		System.out.println("Done! Total number of edges in matrix: "+numOfEdges);
	}
	
	public static void main(String[] args) {
		
		
		
		System.exit(0);
		
		try {
			int numOfAttrs = 262144;
			for (int i=0; numOfAttrs <= Integer.MAX_VALUE/2; i++) {
				numOfAttrs *= 2;
				System.out.println("Generating file for "+numOfAttrs+" attributes...");
				genRandArff(numOfAttrs,"D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff\\rand_data\\"+numOfAttrs+".arff");
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
}
