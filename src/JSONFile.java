import java.util.ArrayList;
import java.util.HashMap;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSONFile {

	static String type;
	static String fileName;
	static String cassandra_url;
	static String source_url;
	static String source_username;
	static String source_password;
	static String source_table;
	static String source_database;
	static String cassandra_keyspace;
	static String cassandra_table;
	static long isColumNameInFirstRow;
	static ArrayList<ColumnMapping> columns=new ArrayList<ColumnMapping>();
	static HashMap<String,Integer> colToSequence=new HashMap<String, Integer>();

	public static void loadMapping(String filePath)
	{
        
		FileReader reader = null;
		try {
			reader = new FileReader(filePath);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			System.out.println("File Not Found: "+filePath);
		}
        
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject=null;
        try {
			jsonObject = (JSONObject) jsonParser.parse(reader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in parsing JSON File");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in parsing JSON File");
			e.printStackTrace();
		}
        type=(String)jsonObject.get("Type");
        fileName=(String)jsonObject.get("File Name");
        source_url=(String)jsonObject.get("Source url");
        source_username=(String)jsonObject.get("Source username");
        source_password=(String)jsonObject.get("Source password");
        source_database=(String)jsonObject.get("Source Database");
        source_table=(String)jsonObject.get("Source Table");
        cassandra_url=(String)jsonObject.get("Cassandra url");
        cassandra_keyspace=(String)jsonObject.get("Cassandra Keyspace");
        cassandra_table=(String)jsonObject.get("Cassandra Table");
        isColumNameInFirstRow= (Long) jsonObject.get("Is ColumnName in first Row");
        JSONArray cols= (JSONArray) jsonObject.get("Columns");
        Iterator colIterator = cols.iterator();
        while(colIterator.hasNext())
        {
        	JSONObject jobj=(JSONObject) (colIterator.next());
        	ColumnMapping colObj=new ColumnMapping();
        	colObj.source_name=(String)jobj.get("Source Column Name");
        	colObj.dest_colName=(String) jobj.get("Destination Column Name");
        	colObj.sequence=(String)jobj.get("Sequence");
        	colObj.dataType=(String)jobj.get("Data Type");
        	colObj.size=(String) jobj.get("Size");
        	colObj.deflt=(String) jobj.get("Default");
        	colToSequence.put(colObj.dest_colName, Integer.parseInt(colObj.sequence));
        	column clm =new column();
        	clm.sourceName=colObj.source_name;
        	clm.name=colObj.dest_colName;
        	clm.dataType=colObj.dataType;
        	tableRecord.columns.add(clm);
        	columns.add(colObj);
        }
        
	}
	public static void display()
	{
		System.out.println(cassandra_url);
		System.out.println(cassandra_table);
		System.out.println(cassandra_keyspace);
		System.out.println(isColumNameInFirstRow);
		for(int i=0;i<columns.size();i++)
		{
			System.out.println(columns.get(i));
		}
		System.out.println("Column mappings");
		for(String col:colToSequence.keySet())
			System.out.println(col+" "+colToSequence.get(col));
	}

}
