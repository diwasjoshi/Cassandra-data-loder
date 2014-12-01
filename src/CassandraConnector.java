import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnector {
   private Cluster cluster;
   private Session session;
   
   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node).build();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() ) {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }
      session=cluster.connect();
   }
      public void loadData()
   {
	   String insertString="INSERT INTO "+JSONFile.cassandra_keyspace+"."+JSONFile.cassandra_table+" (";
	   int i;
	   for(i=0;i<tableRecord.columns.size()-1;i++)
	   {
		   insertString+=tableRecord.columns.get(i).name+", ";
	   }
	   insertString+=tableRecord.columns.get(i).name+") VALUES (";
	   for(i=0;i<tableRecord.columns.size()-1;i++)
	   {
		   if(tableRecord.columns.get(i).dataType.compareTo("String")==0)
		   {
			   insertString+="'"+tableRecord.row.get(tableRecord.columns.get(i).name)+"'"+",";
		   }
		   else 
			   insertString+=tableRecord.row.get(tableRecord.columns.get(i).name)+",";
	   }
	   if(tableRecord.columns.get(i).dataType.compareTo("String")==0)
	   {
		   insertString+="'"+tableRecord.row.get(tableRecord.columns.get(i).name)+"'";
	   }
	   else 
		   insertString+=tableRecord.row.get(tableRecord.columns.get(i).name);
	   insertString+=") ;";
	   System.out.println(insertString);
	   try{
	   session.execute(insertString);
			System.out.println("Loaded data successfully");
	   }
	   catch (Exception e) {

		   System.out.println("Error while inserting: " + e);
	}
   }
   
   public void close() {
      cluster.close();
   }

}