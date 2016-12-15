package GetData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Map;

import GetData.Driver;
import GetData.SortAssesseeForEachAssessor;
import GetData.SortAssessorByTask;

public class GenerateSingleOrder {

	public static Statement myStat;
	
	public static HashMap<String, String> EzTasks;
	public static ArrayList<String> RankCid = new ArrayList<String>();
	public static ArrayList<String> RankAssessor = new ArrayList<String>();
	public static ArrayList<HashMap<String, HashMap<String, String>>> SingleRank = new ArrayList<HashMap<String, HashMap<String, String>>>();
	public String[] tasks;
	
	
	
	public GenerateSingleOrder(){
		
	}
	
	public void Driver(){

		try{
			String url = "jdbc:mysql://peerlogic.csc.ncsu.edu:3306/data_warehouse";
			Connection myConn = (Connection) DriverManager.getConnection(url, "readonly", "readpassword");
			myStat = myConn.createStatement();
		}catch(Exception e){
			e.printStackTrace();
			Driver();
		}
	}
	
	public HashMap<String, String> EZT(){
		EzTasks = new HashMap<String, String>();
		String sql = "select assessee_actor_id , score from answer where create_in_task_id = 'EZ-00001975'";
		try {
			EzTasks = tran_query_into_map(myStat.executeQuery(sql));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return EzTasks;
	}
	
	
	public HashMap<String, String> global() throws SQLException {
		
//		String sql1 = "select DISTINCT id from task where app_name = 'Expertiza';";
//		String sql2 = "select id from criterion where type = 'rank';";
		String sql3 = "select DISTINCT assessor_actor_id from answer where criterion_id in (select id from criterion where type = 'rank') AND rank is not null;";
//		myStat.executeQuery(sql3);
//		System.out.println(sql3);
		
//		EzTasks = tran_query_into_array(Driver.myStat.executeQuery(sql1), "id");
//		RankCid = tran_query_into_array(Driver.myStat.executeQuery(sql2), "id");
		// Yang: you should try to query all the reviewer ids from a specific task, right?		
		RankAssessor = tran_query_into_array(myStat.executeQuery(sql3), "assessor_actor_id");
//		System.out.println(RankAssessor);
		
		HashMap<String, Integer> ocurTime = new HashMap<String, Integer>();
		HashMap<String, String> globalScore = new HashMap<>();
		
		for(int i = 0; i < RankAssessor.size(); i++){
			String sql4 = "select assessee_actor_id, rank from answer where " 
					+ "criterion_id in (select id from criterion where type = 'rank') AND rank != 'null' AND assessor_actor_id ='" + RankAssessor.get(i) + "'";
			ResultSet AeForAs_set = myStat.executeQuery(sql4);
			HashMap<String, String> AeForAs_rank = new HashMap<String, String>();
			AeForAs_rank = tran_query_into_map(AeForAs_set);
			
			
			for(String x : AeForAs_rank.keySet()){
				
				if(!globalScore.containsKey(x)){	
					globalScore.put(x, AeForAs_rank.get(x));
					ocurTime.put(x, 1);
				} else {				
					int occur = ocurTime.get(x);
					int tmp = (Integer.valueOf(AeForAs_rank.get(x)) - Integer.valueOf(globalScore.get(x)))/(occur + 1) + Integer.valueOf(globalScore.get(x));
					String temp = String.valueOf(tmp);
					globalScore.put(x, temp);
					int newOcur = ocurTime.get(x) + 1;
					ocurTime.put(x, newOcur);
				}
			}			
		}
		return globalScore;
	}
	
	public ArrayList<HashMap<String, HashMap<String, String>>> singleOrder(){
		try {
			String sql = "select DISTINCT assessor_actor_id from answer where criterion_id in (select id from criterion where type = 'rank') AND rank != 'null'";
			ArrayList<String> as = tran_query_into_array(myStat.executeQuery(sql), "assessor_actor_id");
			for(String x : as){
				String sql1 = "select assessee_actor_id, rank from answer where criterion_id in (select id from criterion where type = 'rank') AND rank != 'null' and assessor_actor_id = '" + x + "'";
				HashMap<String, String> tmp = tran_query_into_map(myStat.executeQuery(sql1));
				HashMap<String, HashMap<String, String>> temp = new HashMap<String, HashMap<String, String>>();
				temp.put(x, tmp);
				SingleRank.add(temp);
			}
		
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return SingleRank;
	}
	
    public static ArrayList<String> tran_query_into_array(ResultSet result, String string_to_get) {
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
	
	public static HashMap<String,String> tran_query_into_map(ResultSet result) {
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
		GenerateSingleOrder order = new GenerateSingleOrder();
		order.Driver();
		
		HashMap<String, String> allase;
		try {
			allase = order.global();
			System.out.println(allase.get("CV-00000146"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//System.out.println(String.valueOf(3.14159).charAt(1));
		//System.out.println(order.global());
		//System.out.println("I m here.");
		//System.out.println(order.singleOrder());
		//System.out.println(order.EZT());
		
	}

}
