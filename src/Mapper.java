import java.sql.SQLException;


public class Mapper {
	DataFile fileObj=null;
	Database database=null;
	boolean isFile;
	public  Mapper(String type,String path)
	{
		if(type.compareToIgnoreCase("file")==0)
		{	
				isFile=true;
				fileObj=new DataFile(path);
		}
		else if(type.compareTo("postgresql")==0)
		{
			isFile=false;
			database=new PostGreSqlConnector();
		}
	}
	tableRecord getNextTableRecord()
	{
		if(isFile)
		{
			tableRecord record=new tableRecord();
			String[] csvCols=fileObj.getNextRecord();
			if(csvCols==null || csvCols.length<tableRecord.columns.size()) return null;
			
			for(int i=0;i<tableRecord.columns.size();i++)
			{
				String colName=tableRecord.columns.get(i).name;
				String val=csvCols[JSONFile.colToSequence.get(colName)-1];
				if(val==null || val.length()==0 || val.compareTo("\"")==0)
				{
					val=JSONFile.columns.get(i).deflt;

				}
				record.row.put(colName, val);
			}
			return record;
		}
		else
		{
			
			tableRecord record=new tableRecord();
			try {
				//database.getNextRecord();
				record.row=database.getNextRecord();
			
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return record;
		}
	}
}
