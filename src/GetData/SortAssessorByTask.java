/**
 * 
 */
package GetData;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import GetData.Driver;
/**
 * @author yifanguo
 *
 */
public class SortAssessorByTask {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static ArrayList task_id;
	public static Map<String, ArrayList<String>> toTaskAssessor = new HashMap<String, ArrayList<String>>();
	public static Map<String, ArrayList<String>> toTaskAssessee = new HashMap<String, ArrayList<String>>();

	
	public SortAssessorByTask() throws SQLException{
		
			Driver myDriver = new Driver();

			ResultSet all_task_id = Driver.myStat.executeQuery("select id from task");
			ArrayList<String> task_id = Driver.tran_queryset_into_array(all_task_id, "id");
			
			for(int i = 0; i < task_id.size(); i++){
				String sql1 = "select assessor_actor_id from answer where create_in_task_id = '" + task_id.get(i) + "';";
				ResultSet result1 = Driver.myStat.executeQuery(sql1);
				ArrayList<String> toTask_assessor = Driver.tran_queryset_into_array(result1, "assessor_actor_id");
				toTaskAssessor.put(task_id.get(i), toTask_assessor);
			}
			
			for(int i = 0; i < task_id.size(); i++){
				String sql2 = "select assessee_actor_id from answer where create_in_task_id = '" + task_id.get(i) + "';";
				ResultSet result2 = Driver.myStat.executeQuery(sql2);
				ArrayList<String> toTask_assessee = Driver.tran_queryset_into_array(result2, "assessee_actor_id");
				toTaskAssessee.put(task_id.get(i), toTask_assessee);
			}
			
		}
	}
