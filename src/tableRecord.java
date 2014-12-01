import java.util.ArrayList;
import java.util.HashMap;
class column
{
	String name ;
	String dataType;
	String sourceName;
}

public class tableRecord {
	static ArrayList<column> columns=new ArrayList<column>();
	static HashMap<String, String> row=new HashMap<String, String>();
	public void display()
	{
		for(String col:row.keySet())
			System.out.println(col+" "+row.get(col));
	}

}
