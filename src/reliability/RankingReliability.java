package reliability;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;



//* Author: Yifan Guo, Yang Song
//* Calculation of reliability per Task of the ranking based system(Critviz).
//* Tested with Create_in_task_id = 'CV-00000015';

// this class should be named RankingTaskReliabilityCalculator
public class RankingReliability {
	
	public Statement myStat;
	
	public HashMap<String, HashMap<String, String>> single;// an Hash with <assessor_id, {<assessee_id, rank>}>
	public ArrayList<String> allAssessors; // all the assessors in this task
	public LinkedHashMap<String, String> globe; // an Hash with <assessee_id, avg(rank)>
	public ArrayList<String> rankTask;
	public ArrayList<Double> reliabilityPerTask;
	private ArrayList<String> globalRankingForTask; //global ranking of the assessees, from high to low.
	private String taskId;
	private static PrintStream output ;

	//constructor method, create AllRankingTasks obj with a task id.
	public RankingReliability(String taskID) {		
		Driver();
		this.allAssessors(taskID);
		this.globalOrder(taskID);
		this.singleOrder(taskID);
		taskId = taskID;
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
	
	//set get all the assessors in this task and add them to allAssessors array.
	public void allAssessors(String TaskID){
		this.allAssessors = new ArrayList<>();
		String sql1 = "select DISTINCT assessor_actor_id from answer where create_in_task_id='"+ TaskID +"' and rank is not null";
		try {
			allAssessors = tran_query_into_array(myStat.executeQuery(sql1), "assessor_actor_id");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//For each assessor in a task, generate a map of assessor--->(assesee, rank).
	public void singleOrder(String TaskID) {
		this.single = new HashMap<>(); 
		try {	
			for(String ase : allAssessors){
				String sql2 = "select assessee_actor_id, rank from answer where create_in_task_id='"+ TaskID +"' and rank is not null and assessor_actor_id='" + ase + "'";
				HashMap<String, String> temp = tran_query_into_map(myStat.executeQuery(sql2)); 
				single.put(ase, temp);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Generate a map of globally assessee--->avg(rank) and put the hashmap globe. 
	// The entries in globe are sorted here -> order by avg(rank) DESC
	public void globalOrder(String TaskID){
		this.globe = new LinkedHashMap<>();
		String sql1 = "select assessee_actor_id, avg(rank) from answer where create_in_task_id='"+ TaskID +"' and rank is not null group by assessee_actor_id order by avg(rank) DESC";
		try {
			globe = tran_query_into_map(myStat.executeQuery(sql1));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//sort all the assessees and put the assessee_ids in order (high to low)
		this.globalRankingForTask = new ArrayList<String>();
		for ( String key : globe.keySet() ) {
			globalRankingForTask.add(key);
		}
	}
	
	//Get the reliability per Assessor.
	//Alg. 1: percentage of matched tuples
	//return the percentage of total tuples which agree with the global ranking.
	public double getReliabilityForAssessorAlgo1(String assessorId){
		double numMachingTuples = 0;
		// get all the ranking records done by this assessor
		HashMap<String, String> reviewRecords = single.get(assessorId); 
		String[] assessees = reviewRecords.keySet().toArray(new String[reviewRecords.size()]);
		
		for(int i = 0; i < assessees.length-1; i++){
			//System.out.println(tuple[i] + "--->" + globe.get(tuple[i]));
			for(int j = i+1; j < assessees.length; j++){

				Integer a1_l = Integer.valueOf(reviewRecords.get(assessees[i])), a2_l = Integer.valueOf(reviewRecords.get(assessees[j]));
				Double a1_g = Double.parseDouble(globe.get(assessees[i])), a2_g = Double.parseDouble(globe.get(assessees[j]));
				
				if((a1_l > a2_l && a1_g > a2_g) || (a1_l < a2_l && a1_g < a2_g )){
					numMachingTuples += 1;
				}
			}
		}
		
		double numTuplesTotal = (assessees.length * (assessees.length-1))/2;
		return numMachingTuples/numTuplesTotal;
	}
	
	//Get the reliability per Assessor.
	//Alg. 2: sum(diff rankings_matching ^2)/sum(diff ranking_all tuples ^2)
	public double getReliabilityForAssessorAlgo2(String assessorId){
		double diffRankingSquaresMachingTuples = 0, diffRankingSquaresAllTuples = 0;
		// get all the ranking records done by this assessor
		HashMap<String, String> reviewRecords = single.get(assessorId); 
		String[] assessees = reviewRecords.keySet().toArray(new String[reviewRecords.size()]);
		
		for(int i = 0; i < assessees.length-1; i++){
			//System.out.println(tuple[i] + "--->" + globe.get(tuple[i]));
			for(int j = i+1; j < assessees.length; j++){

				Integer a1_l = Integer.valueOf(reviewRecords.get(assessees[i])), a2_l = Integer.valueOf(reviewRecords.get(assessees[j]));
				Double a1_g = Double.parseDouble(globe.get(assessees[i])), a2_g = Double.parseDouble(globe.get(assessees[j]));
				
				int diffRankingInGlobal = (globalRankingForTask.indexOf(assessees[i])) - (globalRankingForTask.indexOf(assessees[j]));
				diffRankingSquaresAllTuples += diffRankingInGlobal * diffRankingInGlobal;
				
				if((a1_l > a2_l && a1_g > a2_g) || (a1_l < a2_l && a1_g < a2_g )){
					diffRankingSquaresMachingTuples += diffRankingInGlobal * diffRankingInGlobal;
				}
			}
		}
		
		return diffRankingSquaresMachingTuples/diffRankingSquaresAllTuples;
	}
	
	//Generate average reliability for the list of all assessors in a task.
	public double avgReliabilityForAll(int algoIndex){
		double sumReliability = 0, assessorNum = allAssessors.size(), reliability = 0;
		for(String assessor : allAssessors){
			//get review reliability for current assessor
			if (algoIndex == 1){
				reliability = getReliabilityForAssessorAlgo1(assessor);
			}
			else{
				reliability = getReliabilityForAssessorAlgo2(assessor);
			}
			//out put the reliability for each individual to a file.
			// Format: taskid, assessorid, reliability, algorithm (1 or 2)
			output.println(this.taskId + ',' + assessor + "," + reliability + "," + algoIndex);

			//System.out.println("Assessor: " + assessor + " 's reliability is " + reliability);
			//add the reliability of current assessor to the total.
			sumReliability += reliability;
		}	
		System.out.println("Total reliability on average for ranking based system on this task is " + sumReliability/assessorNum);
		return sumReliability/assessorNum;
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
	private LinkedHashMap<String,String> tran_query_into_map(ResultSet result) {
		LinkedHashMap<String, String> newMap = new LinkedHashMap<String, String>();
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

		AllTasks allRank = new AllTasks();
		
		//create output stream
		try{
			File file = new File("ranking_output.csv");
			output = new PrintStream(file);
		} catch (IOException e) {
		}
		
		// get all the task id with "ranking" as review type.
		for(String s : allRank.rankTask){
			RankingReliability x = new RankingReliability(s);
			System.out.println("Task ID is: " + s);
			//calculate the overall ranking reliability for this ranking task and out put. Parameter should either be 1 or 2 depending on which algo. to use
			Double reliabilityAlgo1 = x.avgReliabilityForAll(1);
			Double reliabilityAlog2 = x.avgReliabilityForAll(2);
			
			System.out.println("---------------------------------------------------------------------");
		}
		System.out.println("done.");
	}

}
