package feature_gen;

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

public class item2matchIdReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		HashMap<Long,Double> coll_info = new HashMap<Long,Double> ();
		HashMap<Long,Double> coll_count = new HashMap<Long,Double> ();
		while (values.hasNext()) {
			Record val = values.next();
			String [] coll_score = val.get(0).toString().split(":");
			long coll_id = Long.parseLong(coll_score[0]);
			double sim_score = Double.parseDouble(coll_score[1]);
			if(coll_info.containsKey(coll_id)){
				coll_info.put(coll_id, coll_info.get(coll_id) + sim_score);
				coll_count.put(coll_id, coll_count.get(coll_id) + 1.0);
			} else{
				coll_info.put(coll_id, sim_score);
				coll_count.put(coll_id, 1.0);
			}
		}
		// get avg score
		Iterator<Long> coll_id_iter = coll_info.keySet().iterator();
		while(coll_id_iter.hasNext()){
			long coll_id = coll_id_iter.next();
			coll_info.put(coll_id,coll_info.get(coll_id)/coll_count.get(coll_id));
		}
		//sort
		List<Map.Entry<Long, Double>> infoIds = 
				new ArrayList<Map.Entry<Long, Double>>(coll_info.entrySet()); 
		Collections.sort(infoIds, new Comparator<Map.Entry<Long, Double>>() {  
            public int compare(Map.Entry<Long, Double> o1,  
                    Map.Entry<Long, Double> o2) {  
                return (o2.getValue()).compareTo(o1.getValue());  
            }  
        });
		int len_top500 = Math.min(500, infoIds.size());
		// top30 feature
		String top500_coll_feature = "";
		for (int ii = 0; ii < len_top500; ii++) {
			String tmp_coll_score = infoIds.get(ii).toString();
			top500_coll_feature += ";";
			top500_coll_feature += tmp_coll_score;
		}
		top500_coll_feature = top500_coll_feature.substring(1);
		
		String top_coll_feature = top500_coll_feature;
		
		result.set(0, key.get(0));
		result.set(1, top_coll_feature);
		context.write(result);
	}
}