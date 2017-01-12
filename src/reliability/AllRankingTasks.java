package reliability;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class AllRankingTasks {
	
	public Statement myStat;
	
	public ArrayList<String> rankTask;
	public ArrayList<Double> reliabilityPerTask;
	
	public AllRankingTasks() {
		this.Driver();
		this.allTaskID();
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
	private void allTaskID() {
		this.rankTask = new ArrayList<>();
		String sql1 = "select DISTINCT create_in_task_id from answer where rank is not null and create_in_task_id in (select id from task where app_name='CritViz')";
		try {
			rankTask = tran_query_into_array(myStat.executeQuery(sql1), "create_in_task_id");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void reliability(){
		double sum = 0;
		// each string in rankTask is a task_id (for review phase) in Critviz
		// this loop go through all the tasks in rankTask and calculate the overall ranking reliability.
		for(String x : rankTask){
			RankingReliability r = new RankingReliability(x);
			sum += r.avgReliabilityForAll();
		}
		System.out.println("---------------------------------");
		System.out.println((double)sum/rankTask.size());
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
