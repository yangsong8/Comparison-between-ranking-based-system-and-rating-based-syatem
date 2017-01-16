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
//* Calculation of reliability per Task of the rating based system(Expertiza).
//* Tested with Create_in_task_id = 'EZ-00001663';

public class RatingReliability {

	public Statement myStat;
	
	public ArrayList<String> allAssessors;
	public LinkedHashMap<String, String> globe;
	public HashMap<String, HashMap<String, String>> single;
	private static PrintStream output;
	private ArrayList<String> globalRankingForTask; 
	private String taskId;
	private AllTasks allRank;
	
	public RatingReliability(String taskId) {
		Driver();
		this.allAssessor(taskId);
		this.globalOrder(taskId);
		this.singleOrder(taskId);
		taskId = taskId;
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
		try {	
			for(String ase : allAssessors){
				String sql2 = "select assessee_actor_id, sum(answer.score) as sum from answer, criterion "
						+ "where answer.criterion_id=criterion.id and criterion.type='rating' "
						+ "and answer.create_in_task_id='"+taskId+"' and answer.assessor_actor_id='"+ase+"' "
						+ "group by answer.assessee_actor_id;";
				HashMap<String, String> temp = tran_query_into_map(myStat.executeQuery(sql2)); 
				single.put(ase, temp);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void globalOrder(String taskId){
		this.globe = new LinkedHashMap<>();
		String sql1 = "select assessee_actor_id, avg(sum) from "
				+ "(select assessee_actor_id, assessor_actor_id, sum(answer.score) as sum "
				+ "from answer, criterion "
				+ "where answer.criterion_id=criterion.id and criterion.type='rating' "
				+ "and answer.create_in_task_id='"+taskId+"' group by answer.assessee_actor_id, answer.assessor_actor_id) "
				+ "sum_per_assessor_assessee "
				+ "group by assessee_actor_id;";
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
	
	private void allAssessor(String taskId){
		String sql1 = "select DISTINCT assessor_actor_id from answer where create_in_task_id='"+ taskId +"' and score is not null";
		try {
			allAssessors = tran_query_into_array(myStat.executeQuery(sql1), "assessor_actor_id");
		} catch (SQLException e) {
			e.printStackTrace();
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
		
		// if the reviewer only reviewes one artifact, this algorithm has to skip this user
		// b/c it is tuple based. (this is different from the algo. for ranking)
		if(assessees.length <=1){
			return Float.NaN;
		}
		
		for(int i = 0; i < assessees.length-1; i++){
			//System.out.println(tuple[i] + "--->" + globe.get(tuple[i]));
			for(int j = i+1; j < assessees.length; j++){

				try{
					Integer a1_l = Integer.valueOf(reviewRecords.get(assessees[i])), a2_l = Integer.valueOf(reviewRecords.get(assessees[j])); 
					Double a1_g = Double.parseDouble(globe.get(assessees[i])), a2_g = Double.parseDouble(globe.get(assessees[j]));
					if((a1_l > a2_l && a1_g > a2_g) || (a1_l < a2_l && a1_g < a2_g ) || (a1_l == a2_l && a1_l!=0 && (a1_g / a2_g)>0.96 && (a1_g / a2_g)<1.04)){
						numMachingTuples += 1;
					}
				} catch (NumberFormatException e){ // try-catch added to deal with dirty data.
					return Float.NaN;
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
		
		// if the reviewer only reviewes one artifact, this algorithm has to skip this user
		// b/c it is tuple based. (this is different from the algo. for ranking)
		if(assessees.length <=1){
			return Float.NaN;
		}
		
		for(int i = 0; i < assessees.length-1; i++){
			//System.out.println(tuple[i] + "--->" + globe.get(tuple[i]));
			for(int j = i+1; j < assessees.length; j++){

				try{
					Integer a1_l = Integer.valueOf(reviewRecords.get(assessees[i])), a2_l = Integer.valueOf(reviewRecords.get(assessees[j])); 
					Double a1_g = Double.parseDouble(globe.get(assessees[i])), a2_g = Double.parseDouble(globe.get(assessees[j]));
					
					int diffRankingInGlobal = (globalRankingForTask.indexOf(assessees[i])) - (globalRankingForTask.indexOf(assessees[j]));
					diffRankingSquaresAllTuples += diffRankingInGlobal * diffRankingInGlobal;
					
					if((a1_l > a2_l && a1_g > a2_g) || (a1_l < a2_l && a1_g < a2_g ) || (a1_l == a2_l && a1_l!=0 && (a1_g / a2_g)>0.96 && (a1_g / a2_g)<1.04)){
						diffRankingSquaresMachingTuples += diffRankingInGlobal * diffRankingInGlobal;
					}
				} catch (NumberFormatException e){ // try-catch added to deal with dirty data.
					return Float.NaN;
				}
			}
		}
		
		return diffRankingSquaresMachingTuples/diffRankingSquaresAllTuples;
	}
	
	//Generate average reliability for the list of all assessors in a task.
	public double avgReliabilityForAll(int algoIndex){
		double sumReliability = 0, assessorNum =0, reliability = 0;
		for(String assessor : allAssessors){
			//get review reliability for current assessor
			if (algoIndex == 1){
				reliability = getReliabilityForAssessorAlgo1(assessor);
			}
			else{
				reliability = getReliabilityForAssessorAlgo2(assessor);
			}

			//check if the reliability is NaN, if not, then
			//add the reliability of current assessor to the total.
			if(!Double.isNaN(reliability)){
				//out put the reliability for each individual to a file.
				// Format: taskid, assessorid, reliability, algorithm (1 or 2)
				output.println(this.taskId + ',' + assessor + "," + reliability + "," + algoIndex);
				sumReliability += reliability;
				assessorNum += 1; //assessorNum counts the assessors who did more than 1 review.
			}
		}	
		System.out.println("Total reliability on average for ranking based system on this task is " + sumReliability/assessorNum);
		return sumReliability/assessorNum;
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
			File file = new File("rating_output.csv");
			output = new PrintStream(file);
		} catch (IOException e) {
		}
		
		// get all the task id with "ranking" as review type.
		for(String s : allRank.rateTask){
			RatingReliability x = new RatingReliability(s);
			System.out.println("Task ID is: " + s);
			//calculate the overall ranking reliability for this ranking task and out put. Parameter should either be 1 or 2 depending on which algo. to use
			Double reliabilityAlgo1 = x.avgReliabilityForAll(1);
			Double reliabilityAlog2 = x.avgReliabilityForAll(2);
			
			System.out.println("---------------------------------------------------------------------");
		}
		System.out.println("done.");
	}


}
