package feature_gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class featNameConvMapper extends MapperBase {
	private Record result;
	private HashMap<String,Double> feature_max_map;
	private HashMap<String,Double> feature_min_map;
	private HashMap<String,Double> feature_mean_map;
	private ArrayList<String> feature_name_arr;
	private HashMap<String,String> feature_idx_map;
	
	public HashMap<String,String> feature_idx_order(ArrayList<String> feature_name_arr){
		String [] feature_name_list = new String [feature_name_arr.size()];
		HashMap<String,String> feature_idx_map = new HashMap<String,String> ();
		Iterator<String> iter = feature_name_arr.iterator();
		int count = 0;
		while(iter.hasNext()){
			String feature_name = iter.next();
			feature_name_list[count++] = feature_name;
		}
		Arrays.sort(feature_name_list); 
		for(int ii = 0; ii < feature_name_list.length; ii++){
			feature_idx_map.put(feature_name_list[ii], Integer.toString(ii));
		}
		return feature_idx_map;
	}

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
		feature_max_map = new HashMap<String,Double> ();
		feature_min_map = new HashMap<String,Double> ();
		feature_mean_map = new HashMap<String,Double> ();
		feature_name_arr = new ArrayList<String> ();
		
		Iterator<Record> rs_train = context.readResourceTable("feature_info_tab_for_train");
		while(rs_train.hasNext()){
			Record rc = rs_train.next();
			String feature_name = rc.get(0).toString();
			double feature_max_val = Double.parseDouble(rc.get(1).toString());
			double feature_min_val = Double.parseDouble(rc.get(2).toString());		
			feature_max_map.put(feature_name, feature_max_val);
			feature_min_map.put(feature_name, feature_min_val);
			feature_name_arr.add(feature_name);
		}
		Iterator<Record> rs_rsl = context.readResourceTable("feature_info_tab_for_rsl");
		while(rs_rsl.hasNext()){
			Record rc = rs_rsl.next();
			String feature_name = rc.get(0).toString();
			double feature_max_val = Double.parseDouble(rc.get(1).toString());
			double feature_min_val = Double.parseDouble(rc.get(2).toString());
			if(feature_max_val > feature_max_map.get(feature_name)){
				feature_max_map.put(feature_name,feature_max_val);
			}
			if(feature_min_val < feature_min_map.get(feature_name)){
				feature_min_map.put(feature_name,feature_min_val);
			}
		}
		feature_idx_map = feature_idx_order(feature_name_arr);
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String final_feature = "";
		String [] feature = record.getString(2).split(",");
		for(int ii = 0; ii < feature.length; ii++){
			if(!feature[ii].contains(":")){
				continue;
			}
			String [] feature_info = feature[ii].split(":");
			String feature_name = feature_info[0];
			String feature_val = feature_info[1];
			String feature_idx = feature_idx_map.get(feature_name);
			final_feature += "," + feature_idx + ":" + feature_val;
		}
		final_feature = final_feature.substring(1);
		
		if(record.get(3)==null){
			result.set(0, record.get(0));
			result.set(1, record.get(1));
			result.set(2, final_feature);
			context.write(result);
		}else{
			result.set(0, record.get(0));
			result.set(1, record.get(1));
			result.set(2, final_feature);
			result.set(3, record.get(3));
			context.write(result);
		}		
	}
}