package data;

import java.sql.*;
import general.*;
import general.Log.STDTypeEnum;;

/**
 * <p>
 * <h1>Dynamic Network Representation in Database</h1><br>
 * Representation of a dynamic network on a MySQL database. The basic tables and fields are:
 * <b>Table <i>transactors</i>:</b><br>
 * <ul>
 * 	<li>
 * 		<i>transactor_id</i>: int(10) unsigned
 * 	</li>
 * 	<li>
 * 		<i>transactor_name</i>: varchar(120)
 * 	</li>
 * </ul>
 * <b>Table <i>transactions</i>:</b><br>
 * <ul>
 * 	<li>
 * 		<i>timestamp</i>: timestamp (format: "YYYY-MM-DD HH:mm:ss")
 * 	</li>
 * 	<li>
 * 		<i>transactor_id</i>: int(10) unsigned (foreign key: transactors->transactor_id)
 * 	</li>
 * 	<li>
 * 		<i>transactee_id</i>: int(10) unsigned (foreign key: transactors->transactor_id)
 * 	</li>
 * 	<li>
 * 		<i>weight</i>: double
 * 	</li>
 * </ul>
 * </p>
 */
public class BasicDatabase {

	/* ******
	 * fields
	 * ******/

	// database connection parameters
	// ------------------------------
	protected String db_user = "root";
	protected String db_pass = "olive123";
	protected String db_url = "jdbc:mysql://localhost";
	protected int db_port = 3307;
	protected String db_name = "enrondb";
	
	// performance parameters
	// ----------------------
	
	// size of db transaction batch
	protected int size_of_batch = 250;
	// number of threads to use (including db connections)
	protected int num_of_threads = 1;
		
	/* ************
	 * constructors
	 * ************/

	/**
	 * 
	 */
	public BasicDatabase() {
		constOut();
	}

	/**
	 * 
	 * @param db_user
	 * @param db_password
	 * @param db_url
	 * @param db_port
	 * @param db_name
	 */
	public BasicDatabase(String db_user, String db_password, String db_url, int db_port, String db_name)  {
		this.db_user = db_user;
		this.db_pass = db_password;
		this.db_url = db_url;
		this.db_port = db_port;
		this.db_name = db_name;
		
		constOut();
	}
	
	/**
	 * 
	 * @param size_of_batch
	 * @param num_of_threads
	 * @throws DynamicNetworkDatabaseException
	 */
	public BasicDatabase(int size_of_batch, int num_of_threads) throws DynamicNetworkDatabaseException {
		if (size_of_batch < 1 || num_of_threads < 1) {
			String msg = "Size of batch and/or number of threads must be bigger than 0.";
			Log.log(msg,STDTypeEnum.STDERR);
			throw new DynamicNetworkDatabaseException("");
		}
		
		this.size_of_batch = size_of_batch;
		this.num_of_threads = num_of_threads;
		
		constOut();
	}
	
	/**
	 * 
	 * @param db_url
	 * @param db_port
	 * @param db_name
	 * @param size_of_batch
	 * @param num_of_threads
	 * @throws DynamicNetworkDatabaseException
	 */
	public BasicDatabase(String db_user, String db_password, String db_url, int db_port, String db_name, int size_of_batch, int num_of_threads)
			throws DynamicNetworkDatabaseException {
		this.db_user = db_user;
		this.db_pass = db_password;
		this.db_url = db_url;
		this.db_port = db_port;
		this.db_name = db_name;
		
		if (size_of_batch < 1 || num_of_threads < 1) {
			String msg = "Size of batch and/or number of threads must be bigger than 0.";
			Log.log(msg,STDTypeEnum.STDERR);
			throw new DynamicNetworkDatabaseException("");
		}
		
		this.size_of_batch = size_of_batch;
		this.num_of_threads = num_of_threads;
		
		constOut();
	}

	/**
	 * log message after constructor is done
	 */
	public void constOut() {
		Log.log("Created DynamicNetworkDatabase with:\n"+
				"\tdatabase user:\t"+db_user+"\n"+
				"\tdatabase pass:\t"+db_pass+"\n"+
				"\tdatabase url:\t"+db_url+"\n"+
				"\tdatabase port:\t"+db_port+"\n"+
				"\tdatabase name:\t"+db_name+"\n"+
				"\tsize of batch:\t"+size_of_batch+"\n"+
				"\tnum_of_threads:\t"+num_of_threads,
				STDTypeEnum.STDOUT);
	}
	
	
	/* ******************
	 * Main - for testing
	 * ******************/
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Log.log("Starting test ...",STDTypeEnum.STDOUT);
		
		BasicDatabase dndb = new  BasicDatabase();
		
		Log.closeLog();
	}
	
	/**
	 * class for exceptions
	 */
	@SuppressWarnings("serial")
	public class DynamicNetworkDatabaseException extends Exception {
		public DynamicNetworkDatabaseException(String message){
			super(message);
		}
	}

	/* *******************
	 * getters and setters
	 * *******************/
	
	public String get_db_url() {
		return db_url;
	}

	public void set_db_url(String db_url) {
		this.db_url = db_url;
	}

	public int get_db_port() {
		return db_port;
	}

	public void set_db_port(int db_port) {
		this.db_port = db_port;
	}

	public String get_db_name() {
		return db_name;
	}

	public void set_db_name(String db_name) {
		this.db_name = db_name;
	}

	public int get_size_of_batch() {
		return size_of_batch;
	}

	public void set_size_of_batch(int size_of_batch) {
		this.size_of_batch = size_of_batch;
	}

	public int get_num_of_threads() {
		return num_of_threads;
	}

	public void set_num_of_threads(int num_of_threads) {
		this.num_of_threads = num_of_threads;
	}

}
