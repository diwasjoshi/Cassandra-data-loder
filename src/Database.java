import java.sql.SQLException;
import java.util.HashMap;


public abstract class Database {
	abstract HashMap<String,String> getNextRecord() throws SQLException;
}
