package Data_Analys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class catClusterReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	public boolean check_if_same_element(String src_str, String dest_str){
		String [] src_str_list = src_str.split(";");
		String [] dest_str_list = dest_str.split(";");
		for(int ii = 0; ii < src_str_list.length; ii++){
			for(int jj = 0; jj < dest_str_list.length; jj++){
				if(src_str_list[ii].equals(dest_str_list[jj])){
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		ArrayList<String> cat_ids_arr = new ArrayList<String> ();
		while(values.hasNext()){
			String cat_ids = values.next().get(0).toString();
			cat_ids_arr.add(cat_ids);
		}
		String [] cat_ids_list = new String [cat_ids_arr.size()];
		Iterator<String> cat_ids_iter = cat_ids_arr.iterator();
		int count = 0;
		while(cat_ids_iter.hasNext()){
			cat_ids_list[count++] = cat_ids_iter.next();
		}
		
		for(int ii = 0; ii < cat_ids_list.length; ii++){
			if(cat_ids_list[ii].equals("null")){
				continue;
			}
			for(int jj = ii + 1; jj < cat_ids_list.length; jj++){
				if(cat_ids_list[jj].equals("null")){
					continue;
				}
				if(check_if_same_element(cat_ids_list[ii],cat_ids_list[jj]) == false){
					continue;
				}
				HashSet<String> merge_cat_set = new HashSet<String> ();
				String [] cat_id_list_ii = cat_ids_list[ii].split(";");
				String [] cat_id_list_jj = cat_ids_list[jj].split(";");
				for(int mm = 0; mm < cat_id_list_ii.length; mm++){
					merge_cat_set.add(cat_id_list_ii[mm]);
				}
				for(int mm = 0; mm < cat_id_list_jj.length; mm++){
					merge_cat_set.add(cat_id_list_jj[mm]);
				}
				String new_cat_ids = "";
				Iterator<String> merge_cat_iter = merge_cat_set.iterator();
				while(merge_cat_iter.hasNext()){
					new_cat_ids += ";" + merge_cat_iter.next();
				}
				new_cat_ids = new_cat_ids.substring(1);
				cat_ids_list[ii] = new_cat_ids;
				cat_ids_list[jj] = "null";
			}
		}
		
		for(int ii = 0; ii < cat_ids_list.length; ii++){
			if(cat_ids_list[ii].equals("null")){
				continue;
			}
			result.set(0, cat_ids_list[ii]);
			context.write(result);
		}
	}
}