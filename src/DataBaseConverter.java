import java.sql.*;
import java.util.Date;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
//import com.mongodb.MongoException; //jars missing for this exception

/**
 * 
 * @author John Cutsavage
 * 
 * This class implements both MySQL and MongoDB APIs
 * in order to convert an example employee MySQL database
 * into a MongoDB database.
 *
 */
public class DataBaseConverter {
	
	private Connection sqlConn;			//the mysql connection
	private MongoClient mongoClient;	//the mongodb connection
	private MongoDatabase employeeDB;				//new employee database to be created
	

	public static void main(String[] args){
		DataBaseConverter converter = new DataBaseConverter();
		// Initialize MySQL and MongoDB services
		converter.initSQLConnection("root", "password");		// Edit this per user's login credentials
		converter.initMongoConnection();
		// Begin copying MySQL employees database to MongoDB
		converter.copyEmployees();
		// Terminate connection
		converter.closeSQLConnection();
	}
	
	/**
	 * Creates a new mysql connection on the default host. Provide
	 * the connection with your username and password to log into MySQL
	 * 
	 * @param username: The user's username.
	 * @param password: The user's password.
	 * 
	 */
	public void initSQLConnection(String username, String password){
		String url = "jdbc:mysql://localhost:3306/";	// Default MySQL host and port
		String dbName = "employees";					// The employee database provided by MySQL
		String driver = "com/mysql.jdbc.Driver";
		try {
			//Class.forName(driver).newInstance();
			// Set the connection to the default host, using the given username
			// and password to login
			sqlConn = DriverManager.getConnection(url+dbName,username,password);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Closes the MySQL connection.
	 */
	public void closeSQLConnection(){
		try{
			sqlConn.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
	} 

	/**
	 * Creates a new MongoDB client using the default host.
	 */
	public void initMongoConnection(){
		try {
			mongoClient = new MongoClient("localhost", 27017);  // This is the default host and port
			employeeDB = mongoClient.getDatabase("employees");			// Creates a new database named "employees"
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void copyEmployees(){
		try {
			// Run a select query of all entries in employees table from MySQL
			Statement statement = sqlConn.createStatement();
			ResultSet res = statement.executeQuery("SELECT * FROM employees");
			
			// Create new table/collection of employees
			employeeDB.createCollection("employees");
			MongoCollection employeeTable = employeeDB.getCollection("employees");
			
			// Iterate through the employees, capture their information, and copy it to MongoDB
			while(res.next()) {
				int empNum = res.getInt("emp_no");
				Date birthDate = res.getDate("birth_date");
				String firstName = res.getString("first_name");
				String lastName = res.getString("last_name");
				Object gender = res.getObject("gender");
				Date hireDate = res.getDate("hire_date");
				
				BasicDBObject copyDocument = new BasicDBObject();
				
				copyDocument.put("emp_no", empNum);
				copyDocument.put("birth_date", birthDate);
				copyDocument.put("first_name", firstName);
				copyDocument.put("last_name", lastName);
				copyDocument.put("gender", gender);
				copyDocument.put("hire_date", hireDate);
				
				employeeTable.insertOne(copyDocument);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
