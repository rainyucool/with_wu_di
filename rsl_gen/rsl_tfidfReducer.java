package rsl_gen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class rsl_tfidfReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		HashMap<Long,Double> item_score_tab = new HashMap<Long,Double> ();
		while (values.hasNext()) {
			Record val = values.next();
			String [] tmp_item_score = val.get(0).toString().split(":");
			long item_id = Long.parseLong(tmp_item_score[0]);
			double sim_score = Double.parseDouble(tmp_item_score[1]);
			item_score_tab.put(item_id, sim_score);
		}
		//sort
		List<Map.Entry<Long, Double>> infoIds = 
				new ArrayList<Map.Entry<Long, Double>>(item_score_tab.entrySet()); 
		Collections.sort(infoIds, new Comparator<Map.Entry<Long, Double>>() {  
            public int compare(Map.Entry<Long, Double> o1,  
                    Map.Entry<Long, Double> o2) {  
                return (o2.getValue()).compareTo(o1.getValue());  
            }  
        });
		// output
		String match_ids = "";
		int len = Math.min(200, infoIds.size());
		for (int ii = 0; ii < len; ii++) {  
            String item_id = infoIds.get(ii).getKey().toString();
            match_ids += ",";
            match_ids += item_id;
        }
		match_ids = match_ids.substring(1);
		result.set(0, key.get(0));
		result.set(1, match_ids);
		context.write(result);
	}
}