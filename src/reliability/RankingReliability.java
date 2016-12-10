package reliability;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

//* Author: Yifan Guo
//* Calculation of reliability per Task of the ranking based system(Critviz).
//* Tested with Create_in_task_id = 'CV-00000015';

public class RankingReliability {
	
	public Statement myStat;
	
	public HashMap<String, HashMap<String, String>> single;
	public ArrayList<String> Ase;
	public HashMap<String, String> globe;
	public ArrayList<String> rankTask;
	public ArrayList<Double> reliabilityPerTask;

	
	public RankingReliability(String TaskID) {		
		Driver();
		this.allAssessor(TaskID);
		this.globalOrder(TaskID);
		this.singleOrder(TaskID);
		
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
	
	public void allAssessor(String TaskID){
		this.Ase = new ArrayList<>();
		String sql1 = "select DISTINCT assessor_actor_id from answer where create_in_task_id='"+ TaskID +"' and rank is not null";
		try {
			Ase = tran_query_into_array(myStat.executeQuery(sql1), "assessor_actor_id");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Generate a map of assessor--->(assesee, rank).
	
	public void singleOrder(String TaskID) {
		this.single = new HashMap<>(); 
		try {	
			for(String ase : Ase){
				String sql2 = "select assessee_actor_id, rank from answer where create_in_task_id='"+ TaskID +"' and rank is not null and assessor_actor_id='" + ase + "'";
				HashMap<String, String> temp = tran_query_into_map(myStat.executeQuery(sql2)); 
				single.put(ase, temp);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Generate a map of globally assessee--->avg(rank).
	
	public void globalOrder(String TaskID){
		this.globe = new HashMap<>();
		String sql1 = "select assessee_actor_id, avg(rank) from answer where create_in_task_id='"+ TaskID +"' and rank is not null group by assessee_actor_id";
		try {
			globe = tran_query_into_map(myStat.executeQuery(sql1));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//System.out.println(globe);
	}
	
	//Get the reliability per Assessor.
	
	public double reliabPerAsr(String asr){
		double reliability = 0;
		HashMap<String, String> tmp = single.get(asr);
		String[] tuple = tmp.keySet().toArray(new String[tmp.size()]);
		
		System.out.println(tmp);
		
		for(int i = 0; i < tuple.length-1; i++){
			//System.out.println(tuple[i] + "--->" + globe.get(tuple[i]));
			for(int j = i+1; j < tuple.length; j++){

				Integer a1_l = Integer.valueOf(tmp.get(tuple[i])), a2_l = Integer.valueOf(tmp.get(tuple[j]));
				Double a1_g = Double.parseDouble(globe.get(tuple[i])), a2_g = Double.parseDouble(globe.get(tuple[j]));
				
				if((a1_l > a2_l && a1_g > a2_g) || (a1_l < a2_l && a1_g < a2_g )){
					reliability += 1;
				}
			}
		}
		
		double allTuple = (tuple.length * (tuple.length-1))/2;
		return reliability/allTuple;
	}
	
	//Generate reliability for the list of all assessors.
	
	public double avgReliabilityForAll(){
		double reliab = 0, Ase_num = Ase.size();
		for(String ase : Ase){
			double tmp = reliabPerAsr(ase);
			reliab += tmp;
			System.out.println("Assessor: " + ase + " 's reliability is " + tmp);
		}	
		System.out.println("Total reliability on average for ranking based system on task 'CV-00000015' is " + reliab/Ase_num);
		return reliab/Ase_num;
	} 
	
	//Helper function: turn resultSet into array given a attribute field.
	private ArrayList<String> tran_query_into_array(ResultSet result, String string_to_get) {
		ArrayList<String> newArray = new ArrayList<String>();
		try {
			while(result.next()){
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
//		String TaskID = "CV-00000015";
		AllRankingTasks allRank = new AllRankingTasks();
		for(String s : allRank.rankTask){
			RankingReliability x = new RankingReliability(s);
			System.out.println("Task ID is: " + s);
			x.avgReliabilityForAll();
			System.out.println("---------------------------------------------------------------------");
		}
	}

}
