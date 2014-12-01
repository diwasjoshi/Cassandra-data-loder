
public class DataLoader {

	public static void main(String[] args) {
		JSONFile.loadMapping(args[0]);
		//JSONFile.display();
		Mapper mapperObj=new Mapper(JSONFile.type,JSONFile.fileName);
		CassandraConnector casObj=new CassandraConnector();
		casObj.connect(JSONFile.cassandra_url);
		tableRecord tbl=mapperObj.getNextTableRecord();
		while(tbl!=null)
		{
			if(tbl.row==null) break;
			//tbl.display();
			casObj.loadData();
			tbl=mapperObj.getNextTableRecord();
		

		}
		System.out.println("Exit");
		casObj.close();
	}

}
