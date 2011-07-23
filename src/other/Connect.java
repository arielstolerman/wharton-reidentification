package other;

import java.sql.*;

public class Connect
{
	public static void printTransactions(Connection conn) throws SQLException {
		Statement s = conn.createStatement ();
		s.executeQuery ("SELECT timestamp, sender_id, recipient_id, weight FROM transactions");
		ResultSet rs = s.getResultSet ();
		int count = 0;
		while (rs.next ())
		{
			Date timestamp = rs.getDate("timestamp");
			int sender_id = rs.getInt("sender_id");
			int recipient_id = rs.getInt("recipient_id");
			double weight = rs.getDouble("weight");
			System.out.println(timestamp+"\t"+sender_id+"\t"+recipient_id+"\t"+weight);
			++count;
		}
		rs.close ();
		s.close ();
		System.out.println (count + " rows were retrieved");
	}
	
	public static void main (String[] args)
	{
		Connection conn = null;

		try
		{
			String userName = "root";
			String password = "olive123";
			String url = "jdbc:mysql://localhost:3307/enrondb";
			Class.forName ("com.mysql.jdbc.Driver").newInstance ();
			conn = DriverManager.getConnection (url, userName, password);
			System.out.println ("Database connection established");

			// try communicate with the db
			//printTransactions(conn);
			
			// update db
			String ex = "INSERT INTO `transactors`(`transactor_name`) VALUES ('username2')\n"+
						"INSERT INTO `transactors`(`transactor_name`) VALUES ('username3')";
					//"INSERT IF NOT EXISTS INTO `transactors`(`transactor_name`) VALUES ('username2')";
			PreparedStatement ps = conn.prepareStatement(ex);
			int count = ps.executeUpdate();
			ps.close();
			System.out.println(count+" rows were inserted");
			
			// print table again
			//printTransactions(conn);
		}
		catch (Exception e)
		{
			System.err.println ("Cannot connect to database server");
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