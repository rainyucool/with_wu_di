package test_item_pair_gen;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class testItemPairReducer extends ReducerBase {
	private Record result;
	private HashMap<Long,HashSet<Long>> cat2cat_tab;
	private HashMap<Long,String> test_item_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load match_cat2cat_tab
		cat2cat_tab = new HashMap<Long, HashSet<Long>>();
		Iterator<Record> rs_cat2cat = context.readResourceTable("match_cat2cat_tab");
		while (rs_cat2cat.hasNext()) {
			Record val = rs_cat2cat.next();
			long cat_id = Long.parseLong(val.get(0).toString());
			String [] cat_ids = val.get(1).toString().split(";");
			HashSet<Long> tmp_cat_set = new HashSet<Long> ();
			for(int ii = 0; ii < cat_ids.length; ii++){
				tmp_cat_set.add(Long.parseLong(cat_ids[ii]));
			}
			cat2cat_tab.put(cat_id, tmp_cat_set);
		}
		// load half_test_item_id
		test_item_tab = new HashMap<Long, String>();
		Iterator<Record> rs_test_item = context.readResourceTable("half_test_item_tab");
		while (rs_test_item.hasNext()) {
			Record val = rs_test_item.next();
			long item_id = Long.parseLong(val.get(0).toString());
			long cat_id = Long.parseLong(val.get(1).toString());
			String item_title = val.get(2).toString();
			test_item_tab.put(item_id, Long.toString(cat_id) + ":" + item_title);
		}
	}
	
	public boolean check_cat_match(long src_cat_id,long dest_cat_id){
		if(!cat2cat_tab.containsKey(src_cat_id)||!cat2cat_tab.containsKey(dest_cat_id)){
			return false;
		}
		if(cat2cat_tab.get(src_cat_id).contains(dest_cat_id)){
			return true;
		}
		return false;
	}
	
	public String rmv_dup_string(String item_title){
		String [] tmp_item_word = item_title.split(",");
		HashSet<String> set_item_word = new HashSet<String> ();
		for(int ii = 0; ii < tmp_item_word.length; ii++){
			set_item_word.add(tmp_item_word[ii]);
		}
		Iterator<String> iter = set_item_word.iterator();
		String rsl_item_title = "";
		while(iter.hasNext()){
			rsl_item_title += ',';
			rsl_item_title += iter.next();
		}
		rsl_item_title = rsl_item_title.substring(1);
		return rsl_item_title;
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		String [] dest_item_info = key.get(0).toString().split(":");
		long dest_item_id = Long.parseLong(dest_item_info[0]);
		long dest_cat_id = Long.parseLong(dest_item_info[1]);
		String dest_item_title = "";
		while (values.hasNext()) {
			Record val = values.next();
			dest_item_title = val.get(0).toString();
		}
		Iterator<Long> test_item_iter = test_item_tab.keySet().iterator();
		while(test_item_iter.hasNext()){
			long src_item_id = test_item_iter.next();
			String [] cat_title = test_item_tab.get(src_item_id).split(":");
			long src_cat_id = Long.parseLong(cat_title[0]);
			String src_item_title = cat_title[1];
			if(check_cat_match(src_cat_id,dest_cat_id)){
				result.set(0,src_item_id);
				result.set(1,src_cat_id);
				result.set(2,rmv_dup_string(src_item_title));
				result.set(3,dest_item_id);
				result.set(4,dest_cat_id);
				result.set(5,rmv_dup_string(dest_item_title));
				context.write(result);
			}
		}
	}
}