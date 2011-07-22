package db;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class EnronDatabase {
	
	public static String DB_USER = "root";
	public static String DB_PASSWORD = "olive123";
	public static String DB_URL = "jdbc:mysql://localhost";
	public static int DB_PORT = 3307;
	public static String DB_NAME = "enrondb";
	public static int NUM_OF_LINES_IN_TRANSACTION = 1024;

	/**
	 * Read input file with transactors data and insert it into the enron sql db.
	 * @param inpath
	 * 				Path for the data file.
	 * @param conn
	 * 				SQL DB connection.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws SQLException
	 */
	public static void putTransactorsInDB(String inpath, Connection conn) throws FileNotFoundException, IOException, SQLException {
		// create read input buffer
		FileInputStream inStream = new FileInputStream(inpath);
		DataInputStream in = new DataInputStream(inStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
				
		// read data and put into map
		int count = 1;
		String line;
		
		while ((line = br.readLine()) != null) {
			//INSERT INTO `transactors`(`transactor_id`, `transactor_name`) VALUES ([value-1],[value-2])
			PreparedStatement ps = conn.prepareStatement("INSERT IGNORE INTO `transactors`(`transactor_id`, `transactor_name`) VALUES ("+count+",'"+line+"')");
			count += ps.executeUpdate();
			ps.close();
			if (count % 100 == 0) System.out.println(count+" rows were inserted");
		}
		System.out.println("==========================");
		System.out.println("total "+count+" rows were inserted");
		
		br.close();
	}
	
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
	
	public static void writeNumericTransactionsFile(String outpath, String inpath, Map<String,Integer> map) throws IOException {
		// create write output buffer
		FileOutputStream outStream = new FileOutputStream(outpath);
		DataOutputStream out = new DataOutputStream(outStream);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		// create read input buffer
		FileInputStream inStream = new FileInputStream(inpath);
		DataInputStream in = new DataInputStream(inStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// read transaction data, map and write to file
		int count = 0;
		int outputCount = 1;
		String line;
		String[] split;
		String newline;
		
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
		
		System.out.println("==========================");
		System.out.println("total "+count+" rows were written");
		
		bw.flush();
		bw.close();
		br.close();
	}
	
	/**
	 * Main - run DB actions
	 * @param args
	 */
	public static void main (String[] args)
	{
		Connection conn = null;

		try
		{
			Class.forName ("com.mysql.jdbc.Driver").newInstance ();
			String url = DB_URL+":"+DB_PORT+"/"+DB_NAME;
			conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
			System.out.println ("Database connection established");
			
			/*
			 * Do DB actions
			 */
			
			String path = "D:\\data\\workspace\\java\\wharton.reidentification\\raw-data\\enron complete database\\out\\final\\";
			//putTransactorsInDB(path+"transactors.txt",conn);
			Map<String,Integer> map = getTransactorMap(conn);
			writeNumericTransactionsFile(path+"transactions_numeric_ids.txt",path+"transactions.txt",map);

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
			if (conn != null)
			{
				try
				{
					conn.close ();
					System.out.println ("Database connection terminated");
				}
				catch (Exception e) { /* ignore close errors */ }
			}
		}
	}
}
