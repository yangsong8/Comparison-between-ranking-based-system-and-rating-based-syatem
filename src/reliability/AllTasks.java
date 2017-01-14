package reliability;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

//* Author: Yifan Guo, Yang Song
// Get all the task_ids of different type.
// Currently we got only ranking and rating, each of them got one system, 
// but will be extended in the future.

public class AllTasks {
	
	public Statement myStat;
	
	public ArrayList<String> rankTask;
	public ArrayList<String> rateTask;
	public ArrayList<Double> reliabilityPerTask;
	
	public AllTasks() {
		this.Driver();
		this.getAllTaskID("ranking");
		this.getAllTaskID("ranking");
	}

	// This is a bad naming. Should be "setDBConnection"
	// Connect to peerlogic data warehouse with user id and password.
	private void Driver(){
		try{
			String url = "jdbc:mysql://peerlogic.csc.ncsu.edu:3306/data_warehouse";
			Connection myConn = (Connection) DriverManager.getConnection(url, "readonly", "readpassword");
			myStat = myConn.createStatement();
			System.out.println("Connected.");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	// should be named "getAllRankingTaskID"
	// get all the task_id in answer table related to Critviz
	private void getAllTaskID(String taskType) {
		String systemNames;
		// there will be more than 1 system for each type. So I put them in Parentheses.
		// E.g. the systems can be ('Critviz', 'MobiusSLIP') for ranking in the future.  --Yang
		if (taskType.equals("ranking")){
			systemNames = "('CritViz')";
		}
		else{//rating
			systemNames = "('Expertiza')";
		}
		
		this.rankTask = new ArrayList<>();
		String sql1 = "select DISTINCT create_in_task_id from answer where rank is not null and create_in_task_id in (select id from task where app_name in "+ systemNames +" )";
		try {
			if (taskType.equals("ranking")){
				rankTask = tran_query_into_array(myStat.executeQuery(sql1), "create_in_task_id");
			}
			else{//rating
				rateTask = tran_query_into_array(myStat.executeQuery(sql1), "create_in_task_id");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	//Helper function: turn resultSet into array (of Strings) given an attribute field.
	private ArrayList<String> tran_query_into_array(ResultSet result, String string_to_get) {
		ArrayList<String> newArray = new ArrayList<String>();
		try {
			while(result.next())
			{
				newArray.add(result.getString(string_to_get));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
			return newArray;	
	}

}
