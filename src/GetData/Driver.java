package GetData;

import java.sql.*;
import java.util.*;

public class Driver {
	
	public static Statement myStat;
	
	public Driver(){

		try{
			String url = "jdbc:mysql://peerlogic.csc.ncsu.edu:3306/data_warehouse";
			Connection myConn = (Connection) DriverManager.getConnection(url, "readonly", "readpassword");
			myStat = myConn.createStatement();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		
		try{
			SortAssessorByTask AssessorByTask = new SortAssessorByTask();
			SortAssesseeForEachAssessor AssesseeForAssessor = new SortAssesseeForEachAssessor();		

		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
	public static ArrayList tran_queryset_into_array(ResultSet result, String string_to_get) throws SQLException {
		ArrayList<String> newArray = new ArrayList();
		while(result.next())
		{
			newArray.add(result.getString(string_to_get));
		}
		return newArray;
		
	}
	
}
