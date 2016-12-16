package reliability;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

//* Author: Yifan Guo
//* Calculation of reliability per Task of the rating based system(Expertiza).
//* Tested with Create_in_task_id = 'EZ-00001663';

public class RatingReliability {

	public Statement myStat;
	
	public ArrayList<String> Ase;
	public HashMap<String, String> globe;
	public HashMap<String, HashMap<String, String>> single;
	
	public RatingReliability(String taskId) {
		Driver();
		this.allAssessor(taskId);
		this.globalOrder(taskId);
		this.singleOrder(taskId);
	}

	public void Driver(){
		try{
			String url = "jdbc:mysql://peerlogic.csc.ncsu.edu:3306/data_warehouse";
			Connection myConn = (Connection) DriverManager.getConnection(url, "readonly", "readpassword");
			myStat = myConn.createStatement();
			System.out.println("Connected.");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void singleOrder(String taskId){
		this.single = new HashMap<>();
		for(String ase : Ase){
			String sql1 = "select assessee_actor_id, sum(score) from answer where create_in_task_id='"+ taskId +"' and assessor_actor_id='" +ase+ "' group by assessee_actor_id";
			try {
				HashMap<String, String> temp = tran_query_into_map(myStat.executeQuery(sql1));
				single.put(ase, temp);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void globalOrder(String taskId){
		this.globe = new HashMap<>();
		String sql1 = "select  assessee_actor_id, sum(score) from answer where create_in_task_id='"+ taskId +"' group by assessee_actor_id";
		try {
			globe = tran_query_into_map(myStat.executeQuery(sql1));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void allAssessor(String taskId){
		String sql1 = "select DISTINCT assessor_actor_id from answer where create_in_task_id='"+ taskId +"' and score is not null";
		try {
			Ase = tran_query_into_array(myStat.executeQuery(sql1), "assessor_actor_id");
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}
	
	public double reliabPerAsr(String asr){
		double reliability = 0;
		HashMap<String, String> tmp = single.get(asr);
		System.out.println(tmp);
		double allTuple = 0;
		if(tmp.size() > 1){
			
			String[] tuple = tmp.keySet().toArray(new String[tmp.size()]);
			
			for(int i = 0; i < tuple.length-1; i++){
				for(int j = i+1; j < tuple.length; j++){

					Integer a1_l = Integer.parseInt(tmp.get(tuple[i])), a2_l = Integer.parseInt(tmp.get(tuple[j]));
					Integer a1_g = Integer.parseInt(globe.get(tuple[i])), a2_g = Integer.parseInt(globe.get(tuple[j]));
					
					if((a1_l >= a2_l && a1_g >= a2_g) || (a1_l <= a2_l && a1_g <= a2_g)){
						reliability += 1;
					}
				}
			}
			allTuple = ((tuple.length-1)*tuple.length)/2;
		}
		return reliability/allTuple;
	}
	
	public double avgReliabilityForAll(){
		double avgReliab = 0, i = 0;
		for(String asr : Ase){
			if(single.get(asr).size() > 1){
				double tmp = reliabPerAsr(asr);
				System.out.println("Assessor: " + asr + " has the reliability :" + tmp);
				avgReliab += tmp;
				i++;
			}
		}
		return avgReliab/i;
	}
	
	//Helper function: turn resultSet into array given a attribute field.
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
		
	//Helper function: turn resultSet into map given a <key, value> set.	
	private HashMap<String,String> tran_query_into_map(ResultSet result) {
		HashMap<String, String> newMap = new HashMap<String, String>();
		try{	
	    	while(result.next()){
				newMap.put(result.getString(1), result.getString(2));
			}
	  	} catch(Exception e){
			e.printStackTrace();
		}
		return newMap;
	}
	
	public static void main(String[] args) {
		String TaskID = "EZ-00001663";
		RatingReliability r = new RatingReliability(TaskID);
		System.out.println("Average reliability of rating-based System on task: " + TaskID+ " is " + r.avgReliabilityForAll());
		//System.out.println(r.avgReliabilityForAll());
	}

}
