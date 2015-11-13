package feature_gen;

import java.io.IOException;
import java.util.HashMap;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class rslFeatNameConvMapper extends MapperBase {
	private Record result;
	private HashMap<String,String> featureNameMap;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
		featureNameMap = new HashMap<String,String> ();
		featureNameMap.put("min_gap_time","0"); featureNameMap.put("max_gap_time","1");
		featureNameMap.put("mean_gap_time","2"); featureNameMap.put("median_gap_time","3");
		featureNameMap.put("gap_time_std","4"); featureNameMap.put("total_count","5");
		featureNameMap.put("count_0_day","6"); featureNameMap.put("count_3_day","7");
		featureNameMap.put("count_7_day","8"); featureNameMap.put("count_15_day","9");
		featureNameMap.put("ratio_0_day","6"); featureNameMap.put("ratio_3_day","7");
		featureNameMap.put("ratio_7_day","8"); featureNameMap.put("ratio_15_day","9");
		featureNameMap.put("sim_score","10"); featureNameMap.put("tfidf_score","11");
		featureNameMap.put("word_probab","12"); featureNameMap.put("num_same_word","13");		
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
			String feature_idx = featureNameMap.get(feature_name);
			final_feature = "," + feature_idx + ":" + feature_val;
		}
		final_feature = final_feature.substring(1);
		
		result.set(0, record.get(0));
		result.set(1, record.get(1));
		result.set(2, final_feature);
		context.write(result);
	}
}