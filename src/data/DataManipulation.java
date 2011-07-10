package data;

import java.io.*;
import java.lang.management.MemoryUsage;
import java.text.*;
import java.util.*;
import weka.core.*;
import weka.core.converters.ArffLoader;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.*;
import weka.classifiers.lazy.*;
import weka.classifiers.trees.*;

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
			if (i % 100 == 0) {
				System.out.println("- Generated "+i+" vectors...");
				bw.flush();
			}
		}
		bw.flush();
		out.close();
		
		System.out.println("Done! Total number of edges in matrix: "+numOfEdges);
	}
	
	public static ArffLoader getLoader(String arffPath) throws IOException {
		ArffLoader loader = new ArffLoader();
		loader.setFile(new File(arffPath));
		return loader;
	}
	
	public static String getAvailMem() {
		return Long.toString(Runtime.getRuntime().freeMemory()/1048576)+"mb";
	}
	
	public static String getUsedMem() {
		return Long.toString((Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1048576)+"mb";
	}
		
	public static void testClassifier(weka.classifiers.Classifier c, int initialNumAttrs, int maxNumAttrs, boolean evaluate) {
		String arffDir = "D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff\\rand_data\\";
		
		try {
			// run weka analysis on models
			long maxMem = Runtime.getRuntime().maxMemory()/1048576;
			System.out.println(">>> Max JVM memory: "+maxMem+"mb");
			System.out.println("");
			
			int numOfAttrs = initialNumAttrs;
			do {
				Runtime.getRuntime().gc();
				System.out.println("\nAnalysis for "+numOfAttrs+" instances");
				System.out.println("=============================================================================");
				
				try {
					// read data and set class attribute
					System.out.println(">>> Pre read used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
					BufferedReader reader = new BufferedReader(new FileReader(arffDir+numOfAttrs+".arff"));
					Instances data = new Instances(reader);
					reader.close();
					data.setClassIndex(0);
					System.out.println(">>> Post read used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
					System.out.println("Read data with "+data.numInstances()+" instances");
					System.out.println("");

					// build model
					System.out.println("Building model...");
					long initTime = System.currentTimeMillis();
					c.buildClassifier(data);
					System.out.println("Done training! model built in "+(((double)(System.currentTimeMillis() - initTime)) / 1000)+" seconds");
					System.out.println(">>> Post training used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");

					if (evaluate) {
						// evaluate on training set
						Evaluation eval = new Evaluation(data);
						System.out.println("Testing Naive Bayes model on training data...");
						int count = 0;
						for (Iterator<Instance> it = data.iterator(); it.hasNext(); ) {
							eval.evaluateModelOnce(c, it.next());
							count++;
							if (count % 100 ==0) System.out.println("- Evaluated "+count+" instances...");
						}
						System.out.println(">>> Post Naive Bayes evaluating used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
						System.out.println("Evaulation summary:");
						System.out.println("- correct: "+eval.correct()+" ("+(eval.correct()*100/numOfAttrs)+"%)");
						System.out.println("- incorrect: "+eval.incorrect()+" ("+(eval.incorrect()*100/numOfAttrs)+"%)");
						System.out.println();
					}

				} catch (OutOfMemoryError oome) {
					System.err.println("\nOut of memory in last action. Continuing to next iteration...\n");
				}

				numOfAttrs *= 2;
			} while (numOfAttrs <= maxNumAttrs);

		} catch (IOException ioe) {
			System.err.println("IO exception thrown");
			ioe.printStackTrace();
		} catch (Exception e) {
			System.err.println("Exception thrown - propbably from classifier training");
			e.printStackTrace();
		}
	}
	
	public static void testClassifierInc(weka.classifiers.UpdateableClassifier uc, int initialNumAttrs, int maxNumAttrs) {
		String arffDir = "D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff\\rand_data\\";
		
		try {
			// run weka analysis on models
			long maxMem = Runtime.getRuntime().maxMemory()/1048576;
			System.out.println(">>> Max JVM memory: "+maxMem+"mb");
			System.out.println("");
			
			int numOfAttrs = initialNumAttrs;
			do {
				Runtime.getRuntime().gc();
				System.out.println("Analysis for "+numOfAttrs+" instances");
				System.out.println("=============================================================================");
				
				try {
					// read data and set class attribute
					System.out.println(">>> Pre read used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
					BufferedReader reader = new BufferedReader(new FileReader(arffDir+numOfAttrs+".arff"));
					Instances data = new Instances(reader);
					reader.close();
					data.setClassIndex(0);
					System.out.println(">>> Post read used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
					System.out.println("Read data with "+data.numInstances()+" instances");
					System.out.println("");
					
					Evaluation eval = new Evaluation(data);

					// build model incrementally
					int count = 0;
					System.out.println("Building model incrementally...");
					for (Iterator<Instance> it = data.iterator(); it.hasNext(); ) {
						uc.updateClassifier(it.next());
						count++;
						if (count % 100 ==0) System.out.println("- Parsed "+count+" instances...");
					}
					System.out.println("Done training!");
					System.out.println(">>> Post training used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");

					// evaluate on training set
					System.out.println("Testing Naive Bayes model on training data...");
					count = 0;
					weka.classifiers.Classifier c = (weka.classifiers.Classifier) uc;
					for (Iterator<Instance> it = data.iterator(); it.hasNext(); ) {
						eval.evaluateModelOnce(c, it.next());
						count++;
						if (count % 100 ==0) System.out.println("- Evaluated "+count+" instances...");
					}
					System.out.println(">>> Post Naive Bayes evaluating used memory: "+getUsedMem()+" (Memory left: "+getAvailMem()+")");
					System.out.println("Evaulation summary:");
					System.out.println("- correct: "+eval.correct()+" ("+(eval.correct()*100/numOfAttrs)+"%)");
					System.out.println("- incorrect: "+eval.incorrect()+" ("+(eval.incorrect()*100/numOfAttrs)+"%)");
					System.out.println();
					
				} catch (OutOfMemoryError oome) {
					System.err.println("\nOut of memory in last action. Continuing to next iteration...\n");
				}

				numOfAttrs *= 2;
			} while (numOfAttrs <= maxNumAttrs);

		} catch (IOException ioe) {
			System.err.println("IO exception thrown");
			ioe.printStackTrace();
		} catch (Exception e) {
			System.err.println("Exception thrown - propbably from classifier training");
			e.printStackTrace();
		}
	}
		
	public static void genExamples(int initialNumAttrs) {
		String arffDir = "D:\\data\\documents\\Workspace\\wharton.reidentification\\raw-data\\arff\\rand_data\\";
		
		try {
			// generate examples
			int numOfAttrs = initialNumAttrs;
			for (int i=0; numOfAttrs <= Integer.MAX_VALUE/2; i++) {
				System.out.println("Generating file for "+numOfAttrs+" attributes...");
				genRandArff(numOfAttrs,arffDir+numOfAttrs+".arff");
				numOfAttrs *= 2;
			}
		} catch (IOException ioe) {
			System.err.println("IO exception thrown");
			ioe.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//System.out.println("\n##### IBK #####\n");
		//testClassifier(new IBk(5),1024,65536,false);
		
		//System.out.println("\n##### Naive Bayes #####\n");
		//testClassifier(new NaiveBayes(),8192,65536,false);

		//testClassifier(new NaiveBayes(), 65536, 65536);
		//testClassifierInc(new NaiveBayesUpdateable(), 65536, 65536);
		genExamples(524288);
	}
	
}
