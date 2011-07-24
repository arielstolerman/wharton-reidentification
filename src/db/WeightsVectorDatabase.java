package db;

import java.io.*;
import java.lang.Thread.State;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import general.Log;
import general.Log.STDTypeEnum;
import data.*;
import data.BasicDatabase.BasicDatabaseException;

/**
 * Class for creating and modifying a database representation of weights vector.
 *
 */
public class WeightsVectorDatabase {
	
	protected static final String genericTableName = "<WEIGHTS_VECTOR_TABLE_NAME>";
	protected static String wvTableName;
	protected static int size_of_batch;

	/* ********************************************************
	 * creating weights-vector table in existing basic database
	 * ********************************************************/
	
	public static void createWeightVectorTable(
			BasicDatabase db,					// database to use / modify
			String wvTableName					// new weights-vector table name, as will be set in the database
			) {
		
		Log.log(">>> WeightsVectorDatabase.createWeightVectorTable started - table name: "+wvTableName+", database: "+db.get_db_name(), STDTypeEnum.STDOUT);
		
		WeightsVectorDatabase.wvTableName = wvTableName;
		WeightsVectorDatabase.size_of_batch = db.get_size_of_batch();
		
		int num_of_threads = db.get_num_of_threads();
		Connection[] conns = new Connection[num_of_threads];
		int i;
		
		try {
			
			// create table
			// ============
			
			// get sql creation commands
			List<String> createCommands = getCreateTableCommand(wvTableName);
			
			// establish connection
			Log.log("Establishing "+num_of_threads+" connections with database",STDTypeEnum.STDOUT);
			for (i=0; i<num_of_threads; i++)
				conns[i] = db.getNewConnection();
			Statement st = conns[0].createStatement();
			
			// create batch command
			Log.log("Creating batch commands for table creation",STDTypeEnum.STDOUT);
			for (String cmd: createCommands) {
				Log.log("\t"+cmd,STDTypeEnum.STDOUT);
				st.addBatch(cmd);
			}
			
			// run creation batch command
			Log.log("Executing table creation batch commands", STDTypeEnum.STDOUT);
			int[] createCount = st.executeBatch();
			st.clearBatch();
			st.close();
			
			// fill table
			// ==========
			
			// calculate start and end rows for different threads
			int num_of_transactions = db.getCount(conns[0], "transactions");
			int size_of_norm_batch = num_of_transactions/num_of_threads;
			int size_of_last_batch = num_of_transactions - size_of_norm_batch*(num_of_threads - 1);
			
			Log.log("creating "+num_of_threads+" table fill threads", STDTypeEnum.STDOUT);
			FillTableThread[] threads = new FillTableThread[num_of_threads];
			for (i=0; i<num_of_threads-1; i++) {
				// all threads but last
				threads[i] = new FillTableThread(i,i*size_of_norm_batch,(i+1)*size_of_norm_batch-1, conns[i]);
			}
			// last thread
			threads[i] = new FillTableThread(i, i*size_of_norm_batch, i*size_of_norm_batch+size_of_last_batch-1, conns[i]);
			
			// run threads
			Log.log("running "+num_of_threads+" table fill threads", STDTypeEnum.STDOUT);
			for (i =0; i < num_of_threads; i++) threads[i].start();
			for (i =0; i < num_of_threads; i++) threads[i].join();	
			
		} catch (FileNotFoundException e) {
			Log.log("Could not find resource WeightsVectorDatabase.sql", STDTypeEnum.STDERR);
			e.printStackTrace();
			
		} catch (IOException e) {
			Log.log("Encountered an IOException when trying to read from resource WeightsVectorDatabase.sql", STDTypeEnum.STDERR);
			e.printStackTrace();
			
		} catch (SQLException e) {
			Log.log("SQLException thrown. Either add batch command or execution failed. Check resource script and that table doesn't already exist in database",STDTypeEnum.STDERR);
			e.printStackTrace();
			
		} catch (InterruptedException e) {
			Log.log("InterruptedException thrown when trying to join all table fill threads", STDTypeEnum.STDERR);
			e.printStackTrace();
			
		} finally {
			for (i=0; i<num_of_threads; i++) {
				if (conns[i] != null) {
					try {
						conns[i].close ();
						Log.log("Database connection terminated", STDTypeEnum.STDOUT);
						Log.log(">>> WeightsVectorDatabase.createWeightVectorTable ended - table name: "+wvTableName+", database: "+db.get_db_name(), STDTypeEnum.STDOUT);
					} catch (Exception e) { /* ignore close errors */ }
				}
			}
		}
		
	}
	
	// helpers
	// -------
	
	/**
	 * Get the commands for creating the weights vector table using the given table name.
	 * @param wvTableName
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected static List<String> getCreateTableCommand(String wvTableName) throws FileNotFoundException, IOException {
		List<String> res = new ArrayList<String>();
		
		// set path to generic table creation sql script
		String root = System.getProperty("user.dir");
		String del = System.getProperty("file.separator");
		root.replace(del, del+del);
		String path = root+del+del+"src"+del+del+"resources"+del+del+"WeightsVectorDatabase.sql";
		
		// iterate over the file and replace generic name with given table name
		BufferedReader br = General.getBufferedReader(path);
		String line;
		String elem = "";
		while ((line = br.readLine()) != null) {
			if (line == "" || line.startsWith("--")) {
				// empty line / comment - skip
				continue;
			} else if (line.endsWith(";")) {
				// end of sql query - newline
				elem += line.replace(genericTableName,wvTableName).replace(";","");
				res.add(elem);
				elem = "";
			} else {
				// continue aggregate sql query in the same line
				elem += line.replace(genericTableName, wvTableName);
			}
		}
		br.close();
		return res;
	}
	
	/**
	 * Thread class for concurrent weights vector table update
	 */
	protected static class FillTableThread extends Thread {
		// id and start / end lines
		protected int id;
		protected int start_line;
		protected int end_line;
		protected Connection conn;
		
		// shared counter for updated values
		protected static AtomicInteger updateCount = new AtomicInteger(0);
		
		// constructor
		protected FillTableThread(int id, int start_line, int end_line, Connection conn) {
			this.id = id;
			this.start_line = start_line;
			this.end_line = end_line;
			this.conn = conn;
		}
		
		public void run() {
			Log.log(">>> WeightsVectorDatabase.FillTableThread.run started - thread "+id+", lines "+start_line+"-"+end_line, STDTypeEnum.STDOUT);
			
			// read and fill
			readAndFill(id,start_line,end_line,conn);
			
			Log.log(">>> WeightsVectorDatabase.FillTableThread.run ended - thread "+id+", lines "+start_line+"-"+end_line, STDTypeEnum.STDOUT);
		}
		
	}
	
	//TODO read method and fill method
	protected static void readAndFill(int id, int start_line, int end_line, Connection conn) {
		Log.log("WeightsVectorDatabase.readAndFill started - thread "+id,STDTypeEnum.STDOUT);
		
		// iterate over batched - read transactions and update data weights vector table
		Statement st = null;
		ResultSet res = null;
		int i;
		Map<String,Double> map = new HashMap<String,Double>();
		
		int num_of_lines = end_line - start_line + 1;
		int num_of_batches = num_of_lines % size_of_batch == 0 ? num_of_lines/size_of_batch : num_of_lines/size_of_batch + 1;
		int size_of_last_batch = num_of_lines - (num_of_batches - 1)*size_of_batch;

		
		try {
			// initialize statement
			st = conn.createStatement();
			
			// iterate over all batches
			for (i=0; i<num_of_batches - 1; i++) {
				// get batch
				res = st.executeQuery("SELECT `transactor_id`,`transactee_id`,`weight` FROM `transactions` LIMIT "+
						(start_line + i*size_of_batch)+
						","+
						((i == num_of_batches - 1) ? (start_line + size_of_last_batch) : (start_line + (i+1)*size_of_batch - 1))
						);
				
				// process batch
				while (res.next()) {
					String key = res.getString("transactor_id")+"_"+res.getString("transactee_id");
					double weight = res.getDouble("weight");
					
					if (map.containsKey(key)) {
						map.put(key,(map.get(key)+weight));
					} else {
						map.put(key, weight);
					}
				}
				
				// update database
				for (String key: map.keySet()) {
					String[] ids = key.split("_");
					String val = map.get(key).toString();
					
					st.addBatch("INSERT INTO `"+wvTableName+"`(`transactor_id`,`transactee_id`,`weight`) VALUES ("+ids[0]+","+ids[1]+","+val+") "+
							"ON DUPLICATE KEY UPDATE `weight`=`weight`+"+val);
				}
				
				int[] updateCount = st.executeBatch();
				st.clearBatch();
				int totalUpdated = 0;
				
				for (int tmp: updateCount) {
					totalUpdated = FillTableThread.updateCount.addAndGet(tmp);
				}
				Log.log("Thread "+((FillTableThread)Thread.currentThread()).id+" updated batch. Total updated: "+totalUpdated,STDTypeEnum.STDOUT);
				
			}
			
			st.close();

		} catch (SQLException e) {
			Log.log("SQLException thrown from readAndFill",STDTypeEnum.STDERR);
			e.printStackTrace();
		}

		Log.log("WeightsVectorDatabase.readAndFill ended - thread "+id,STDTypeEnum.STDOUT);
	}
	
	
	/******************
	 * main for testing
	 * @param args
	 ******************/
	public static void main(String[] args) {
		try {
			createWeightVectorTable(new BasicDatabase(250,8),"wv");
		} catch (BasicDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
