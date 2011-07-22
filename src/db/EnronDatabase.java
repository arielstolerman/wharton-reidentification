package db;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class EnronDatabase {
	
	public static String DB_USER = "root";
	public static String DB_PASSWORD = "olive123";
	public static String DB_URL = "jdbc:mysql://localhost";
	public static int DB_PORT = 3307;
	public static String DB_NAME = "enrondb";
	public static int SIZE_OF_BATCH = 250;

	/**
	 * Read input file with transactors data and insert it into the enron sql db.
	 * @param inpath
	 * @param conn
	 * @param sizeOfBatch
	 * @param clearTableBeforeInsert
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	public static void putTransactorsInDB(String inpath, Connection conn, int sizeOfBatch, boolean clearTableBeforeInsert) throws FileNotFoundException, IOException, SQLException {
		// create read input buffer
		BufferedReader br = getBufferedReader(inpath);
				
		// read data and put into map
		int count = 0;
		String line;
		Statement st = conn.createStatement();
		int[] updateCount;
		int totalInserted = 0;
		
		long startTime = System.nanoTime();

		// clear transactors table
		if (clearTableBeforeInsert) {
			st.executeUpdate("DELETE FROM `transactors` WHERE 1");
			System.out.println("transactors table has been cleared.");
		}
		
		// create batches and insert into db
		while ((line = br.readLine()) != null) {			
			st.addBatch("INSERT IGNORE INTO `transactors`(`transactor_id`, `transactor_name`) VALUES ("+(++count)+",'"+line+"')");
			
			if (count % sizeOfBatch == 0) {
				updateCount = st.executeBatch();
				for (int i: updateCount) { totalInserted += i; };
				System.out.println(totalInserted+" rows insertred");
				st.clearBatch();
			}
		}

		updateCount = st.executeBatch();
		for (int i: updateCount) { totalInserted += i; };
		System.out.println(totalInserted+" rows insertred.");
		st.clearBatch();

		double totalTime = (System.nanoTime() - startTime)/1000000000;

		System.out.println("==========================");
		System.out.println("total "+totalInserted+" rows were inserted");
		System.out.println("total time: "+totalTime+" secs, size of batch: "+sizeOfBatch+" rows/batch");
		
		st.close();
		br.close();
	}
	
	/**
	 * Create a mapping from transactors names to their ids, as determined in the database.
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	public static Map<String,Integer> getTransactorMap(Connection conn) throws SQLException {
		Map<String,Integer> map = new HashMap<String,Integer>();
		
		// read all data
		Statement s = conn.createStatement ();
		s.executeQuery ("SELECT * FROM transactors");
		ResultSet rs = s.getResultSet ();
		int count = 0;
		while (rs.next ())
		{
			int id = rs.getInt("transactor_id");
			String name = rs.getString("transactor_name");
			count++;
			map.put(name, id);
			
			if (count % 100 == 0) System.out.println("Read "+count+" lines");
		}
		rs.close ();
		s.close ();
		System.out.println("==========================");
		System.out.println("total "+count+" rows were read");
		
		return map;
	}
	
	/**
	 * Given an input transactions file and a mapping from transactors names to their ids, write an
	 * output transactions file with ids instead of names.
	 * @param outpath
	 * @param inpath
	 * @param map
	 * @throws IOException
	 */
	public static void writeNumericTransactionsFile(String outpath, String inpath, Map<String,Integer> map) throws IOException {
		// create write output buffer
		BufferedWriter bw = getBufferedWriter(outpath);
		// create read input buffer
		BufferedReader br = getBufferedReader(inpath);

		// read transaction data, map and write to file
		int count = 0;
		String line;
		String[] split;
		String newline;
		
		long startTime = System.nanoTime();
		
		while ((line = br.readLine()) != null) {
			split = line.split(" ");
			// update sender
			split[1] = (map.get(split[1]) == null) ? "NULL" : map.get(split[1]).toString();
			if (split[1] == null) System.err.println("Sender is null for row "+count);
			// update recipient
			split[2] = (map.get(split[2]) == null) ? "NULL" : map.get(split[2]).toString();
			if (split[2] == null) System.err.println("Recipient is null for row "+count);
			// write to file
			newline = split[0];
			for (int i=1; i<split.length; i++) newline += " "+split[i];
			bw.write(newline+"\n");
			
			// count
			count++;
			if (count % 100 == 0) {
				bw.flush();
				System.out.println(count+" rows were written");
			}
		}
		
		bw.flush();
		
		double totalTime = (System.nanoTime() - startTime)/1000000000;
		
		System.out.println("==========================");
		System.out.println("total "+count+" rows were written");
		System.out.println();
		
		bw.close();
		br.close();
	}
	
	public static void writeNumericTransactionsFileAggregated(String outpath, String inpath) throws IOException {
		// create write output buffer
		BufferedWriter bw = getBufferedWriter(outpath);
		// create read input buffer
		BufferedReader br = getBufferedReader(inpath);
		
		// read transaction data, aggregate and write to file
		int count = 0;
		String line;
		String[] lineSplit;
		String[] aggrSplit = null;
		int aggrCount = 0;

		long startTime = System.nanoTime();

		while ((line = br.readLine()) != null) {
			// compare aggregated line and line
			if (aggrSplit == null) {
				// initialize aggregated line
				aggrSplit = line.split(" ");
				
			} else {
				// compare aggregated line and new line
				lineSplit = line.split(" ");
				if (lineSplit.length != 5) {
					System.err.println("BUG!");
				}
				
				String lineOriginalWeight = lineSplit[4];
				lineSplit[4] = aggrSplit[4];
				
				if (Arrays.deepEquals(aggrSplit, lineSplit)) {
					lineSplit[4] = lineOriginalWeight;
					// line fits in signature - aggregate weight
					aggrSplit[4] = (new Integer(Integer.valueOf(aggrSplit[4]) + Integer.valueOf(lineSplit[4]))).toString();
					aggrCount++;
					
				} else {
					// line doesn't fit in signature - write aggregated line to file and initialize
					bw.write(aggrSplit[0]+" "+aggrSplit[1]+" "+aggrSplit[2]+" "+aggrSplit[3]+" "+aggrSplit[4]+"\n");
					if (aggrCount > 0) {
						System.out.println("Aggregation for line "+count+", signature '"+aggrSplit[0]+"',"+aggrSplit[1]+","+aggrSplit[2]+",'"+aggrSplit[3]+"' is "+aggrCount);
						aggrCount = 0;
					}
					count++;
					aggrSplit = line.split(" ");
				}
			}
			
			// flush
			if (count % 100 == 0) {
				bw.flush();
				//System.out.println(count+" rows were written");
			}
		}
		
		// write last
		bw.write(aggrSplit[0]+" "+aggrSplit[1]+" "+aggrSplit[2]+" "+aggrSplit[3]+" "+aggrSplit[4]+"\n");
		bw.flush();

		double totalTime = (System.nanoTime() - startTime)/1000000000;

		System.out.println("==========================");
		System.out.println("total "+count+" rows were written");
		System.out.println();

		bw.close();
		br.close();
	}
	
	/**
	 * Split given file into given number of files
	 * @param inpath
	 * @param outpathPrefix
	 * @param outpathExtension
	 * @param numOfFiles
	 * @throws IOException
	 */
	public static void splitFile(String inpath, String outpathPrefix, String outpathExtension, int numOfFiles) throws IOException {
		// create read input buffer
		BufferedReader br = getBufferedReader(inpath);
		
		int numOfLines = 0;
		while (br.readLine() != null) numOfLines++;
		br.close();
		br = getBufferedReader(inpath);
		
		int sizeOfFile = numOfLines/numOfFiles;
		int sizeOfLastFile = numOfLines - (numOfFiles-1)*sizeOfFile;
		
		BufferedWriter currBW;
		int i, j;
		int currNumOfLines = sizeOfFile;
		
		// write files
		for (i=0; i<numOfFiles; i++) {
			// initialize writer
			currBW = getBufferedWriter(outpathPrefix+i+"_of_"+numOfFiles+"."+outpathExtension);
			
			// set desired number of lines in current output file
			if (i == numOfFiles - 1) {
				// last file
				currNumOfLines = sizeOfLastFile;
			}
			
			// write to file
			for (j=0; j<currNumOfLines; j++) {
				currBW.write(br.readLine()+"\n");
				
				if (j % 100 == 0) {
					currBW.flush();
				}
			}
			currBW.flush();
			currBW.close();
			System.out.println("Wrote file "+(i+1)+" out of "+numOfFiles);
		}
		
		br.close();
	}

	/**
	 * Read input file with transactions data and insert it into the enron sql db.
	 * @param inpath
	 * @param conn
	 * @param sizeOfBatch
	 * @param clearTableBeforeInsert
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void putTransactionsInDB(String inpath, Connection conn, int sizeOfBatch, boolean clearTableBeforeInsert) throws SQLException, IOException {
		// create read input buffer
		BufferedReader br = getBufferedReader(inpath);
		
		int id = ((TransactionsDBWriterThread)Thread.currentThread()).id;
				
		// read data and put into map
		int count = 0;
		String line;
		String[] split;
		Statement st = conn.createStatement();
		int[] updateCount;
		int totalInserted = 0;
		int batchCounter;
		
		long startTime = System.nanoTime();

		// clear transactors table
		if (clearTableBeforeInsert) {
			st.executeUpdate("DELETE FROM `transactions` WHERE 1");
			System.out.println("transactions table has been cleared.");
		}
		
		// create batches and insert into db
		while ((line = br.readLine()) != null) {
			split = line.split(" ");
			
			st.addBatch("INSERT INTO `transactions`(`timestamp`, `sender_id`, `recipient_id`, `type`, `weight`) " +
					"VALUES ('"+split[0]+"',"+split[1]+","+split[2]+",'"+split[3]+"',"+split[4]+")");
			
			if (++count % sizeOfBatch == 0) {
				updateCount = st.executeBatch();
				batchCounter = 0;
				for (int i: updateCount) {
					batchCounter += i;
				}
				totalInserted = TransactionsDBWriterThread.sharedRowCounter.addAndGet(batchCounter);
				System.out.println("Thread "+id+": "+batchCounter+"/"+sizeOfBatch+" rows inserted in last batch, "+totalInserted+" rows insertred");
				st.clearBatch();
			}
		}

		updateCount = st.executeBatch();
		for (int i: updateCount) {
			totalInserted = TransactionsDBWriterThread.sharedRowCounter.addAndGet(i);
			//totalInserted += i;
		}
		System.out.println(totalInserted+" rows insertred.");
		st.clearBatch();

		double totalTime = (System.nanoTime() - startTime)/1000000000;

		System.out.println("==========================");
		System.out.println("total "+totalInserted+" rows were inserted");
		System.out.println("total time: "+totalTime+" secs, size of batch: "+sizeOfBatch+" rows/batch");
		
		st.close();
		br.close();
	}
	
	/**
	 * 
	 * @param inpathNoFileExtension
	 * @param extension
	 * @param conns
	 * @param sizeOfBatch
	 * @param clearTableBeforeInsert
	 * @param numOfThreads
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void putTransactionsInDBThreaded(String inpathNoFileExtension, String extension, Connection[] conns, int sizeOfBatch,
			boolean clearTableBeforeInsert, int numOfThreads) throws SQLException, IOException {

		// clear transactions table
		if (clearTableBeforeInsert) {
			Statement st = conns[0].createStatement();
			st.executeUpdate("DELETE FROM `transactions` WHERE 1");
			st.close();
			System.out.println("transactions table has been cleared.");
		}

		System.out.println("Starting threads...");

		TransactionsDBWriterThread threads[] = new TransactionsDBWriterThread[numOfThreads];

		int i = 0;
		String inpath = null;
		long start = System.nanoTime();

		for (i = 0; i < numOfThreads; i++) {
			inpath = inpathNoFileExtension+i+"_of_"+numOfThreads+"."+extension;
			threads[i] = new TransactionsDBWriterThread(inpath,conns[i],sizeOfBatch,i);
		}
		
		for (i =0; i < numOfThreads; i++) threads[i].start();
		
		try {
			for (i =0; i < numOfThreads; i++) threads[i].join();	
		}
		catch (InterruptedException e) {
			System.err.println("Interrupted Exception:");
			e.printStackTrace();
		};

		System.out.format("Finished after %d miliseconds.", 
				new Long((System.nanoTime() - start) / 1000000));
	}

	/**
	 * Thread class for parallel writing of transactions to database
	 */
	private static class TransactionsDBWriterThread extends Thread {
		final private String inpath;
		final private Connection conn;
		final private int sizeOfBatch;
		final private int id;
		
		private static AtomicInteger sharedRowCounter = new AtomicInteger(0);

		public TransactionsDBWriterThread(String inpath, Connection conn, int sizeOfBatch, int id) {
			this.inpath = inpath;
			this.conn = conn;
			this.sizeOfBatch = sizeOfBatch;
			this.id = id;
		}

		public void run() {
			System.out.println(">>> Thread " + this.id + " started");

			// run db update
			try {
				putTransactionsInDB(inpath,conn,SIZE_OF_BATCH,false);
			} catch (Exception e) {
				System.err.println("Exception:");
				e.printStackTrace();
			} 
			

			System.out.println(">>> Thread " + this.id + " done");
		}
	}
	
	/**
	 * Main - run DB actions
	 * @param args
	 */
	public static void main (String[] args)
	{
		int numOfThreads = 8;
		Connection[] conns = new Connection[numOfThreads];

		try
		{
			Class.forName ("com.mysql.jdbc.Driver").newInstance ();
			String url = DB_URL+":"+DB_PORT+"/"+DB_NAME;
			for (int i=0; i<numOfThreads; i++) {
				conns[i] = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
			}
			System.out.println ("Database connection established");
			
			/* *************
			 * Do DB actions
			 * *************/
			
			String path = "D:\\data\\workspace\\java\\wharton.reidentification\\raw-data\\enron complete database\\out\\final\\";
			//putTransactorsInDB(path+"transactors.txt",conns[0],SIZE_OF_BATCH,true);
			//Map<String,Integer> map = getTransactorMap(conns[0]);
			//writeNumericTransactionsFile(path+"transactions_numeric_ids.txt",path+"transactions.txt",map);
			//writeNumericTransactionsFileAggregated(path+"transactions_numeric_ids_aggr.txt",path+"transactions_numeric_ids.txt");
			//splitFile(path+"transactions_numeric_ids_aggr.txt",path+"transactions_numeric_ids_aggr","txt",numOfThreads);
			//putTransactionsInDBThreaded(path+"transactions_numeric_ids_aggr", "txt", conns, SIZE_OF_BATCH, true, numOfThreads);
		}
		catch (SQLException sqle) {
			System.err.println("SQL Exception");
			sqle.printStackTrace();
		}
		catch (Exception e)
		{
			System.err.println ("Cannot connect to database server");
			e.printStackTrace();
		}
		finally
		{
			for (int i=0; i<numOfThreads; i++) {
				if (conns[i] != null)
				{
					try
					{
						conns[i].close ();
						System.out.println ("Database connection terminated");
					}
					catch (Exception e) { /* ignore close errors */ }
				}
			}
		}
	}
	
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
			

	
	
}
