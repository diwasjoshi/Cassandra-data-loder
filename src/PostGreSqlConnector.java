import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
public class PostGreSqlConnector extends Database {

	static Connection connection ;
	ResultSet rs=null;
	HashMap<String, String> data;
	/*public static void main(String[] args) {
		PostGreSqlConnector pgs=new PostGreSqlConnector();
	}*/
	public PostGreSqlConnector()
	{
		// TODO Auto-generated method stub
		data=new HashMap<String, String>();
		rs=null;
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
 
		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
 
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
 
		connection = null;
 
		try {
 
		
			String connectionString="jdbc:postgresql://"+JSONFile.source_url+"/"+JSONFile.source_database;	
			//System.out.println(JSONFile.source_username+" "+JSONFile.source_password);
			
			connection = DriverManager.getConnection(connectionString,JSONFile.source_username,JSONFile.source_password);
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}
 
		if (connection != null) {
		
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
	}
	public HashMap<String,String> getNextRecord() throws SQLException
	{
		data=new HashMap<String,String>();
		if(rs==null)
		{

			connection.setAutoCommit(false);
			Statement st = connection.createStatement();

		// Turn use of the cursor on.
		
		//	st.setFetchSize(2);
		
			rs = st.executeQuery("SELECT * FROM "+JSONFile.source_table);
			
			return  getNextRecord();
			
		}
		else 
		{	
			if (rs.next()) {
				
				for(int i=0;i<tableRecord.columns.size();i++)
				{
					String name=tableRecord.columns.get(i).name;
					String val=tableRecord.columns.get(i).sourceName;
					data.put(name,rs.getString(val));
				}

				return data;
				// System.out.print("a row was returned.");
			}
			
			else return null;
		}
		
	}
	
}
