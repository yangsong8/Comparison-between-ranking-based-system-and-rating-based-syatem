package GetData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import GetData.Driver;

public class SortAssesseeForEachAssessor {

	public static ArrayList <String> assessor_id = new ArrayList();
	public static Map<String, ArrayList<String>> assessed = new HashMap<String, ArrayList<String>>();
	public static ArrayList<String> assessee_for_every_assessor = new ArrayList();
	
	public SortAssesseeForEachAssessor() throws SQLException {
		Driver myDirver = new Driver();
		
		ResultSet all_assessor = Driver.myStat.executeQuery("select assessor_actor_id from answer group by assessor_actor_id");
		assessor_id = Driver.tran_queryset_into_array(all_assessor, "assessor_actor_id");

		for(int i = 0; i < assessor_id.size(); i++)
		{
			String sql = "select assessee_actor_id from answer where assessor_actor_id ='" + assessor_id.get(i) + "';";
			ResultSet result = Driver.myStat.executeQuery(sql);
			assessee_for_every_assessor = Driver.tran_queryset_into_array(result, "assessee_actor_id");
			assessed.put(assessor_id.get(i), assessee_for_every_assessor);
		}
		
	}
	
}
