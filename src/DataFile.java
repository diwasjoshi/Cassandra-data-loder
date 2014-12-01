import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;


public class DataFile {
	CSVReader csvReader;
	String [] record;
	public DataFile(String path)
	{
		
		try {
			csvReader=new CSVReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("File Not Found: "+ path);
		}
		
	}
	String[] getNextRecord()
	{
		try {
			record=csvReader.readNext();
			if(JSONFile.isColumNameInFirstRow==1)
			{
				record=csvReader.readNext();
				JSONFile.isColumNameInFirstRow=0;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error reading record from file");
		}
		return record;
	}
	
}
