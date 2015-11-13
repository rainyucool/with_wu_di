package feature_gen;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class simCollFeatureMapper extends MapperBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long src_item_id = Long.parseLong(record.get(0).toString());
		Long dest_item_id = Long.parseLong(record.get(2).toString());
		String [] src_coll_info = record.get(1).toString().split(";");
		String [] dest_coll_info = record.get(3).toString().split(";");
		int src_len = src_coll_info.length;
		int dest_len = dest_coll_info.length;
		Integer [] src_coll_id = new Integer [src_len];
		Double [] src_coll_score = new Double [src_len];
		Integer [] dest_coll_id = new Integer [dest_len];
		Double [] dest_coll_score = new Double [dest_len];
		
		for(int ii = 0; ii < src_len; ii++){
			String [] tmp_coll_info = src_coll_info[ii].split("=");
			src_coll_id[ii] = Integer.parseInt(tmp_coll_info[0]);
			src_coll_score[ii] = Double.parseDouble(tmp_coll_info[1]);
		}
		
		for(int ii = 0; ii < dest_len; ii++){
			String [] tmp_coll_info = dest_coll_info[ii].split("=");
			dest_coll_id[ii] = Integer.parseInt(tmp_coll_info[0]);
			dest_coll_score[ii] = Double.parseDouble(tmp_coll_info[1]);
		}
		
		int feature_num = 8;
		Integer [] N_array = new Integer[feature_num];
		N_array[0] = 30; N_array[1] = 70; N_array[2] = 150; 
		N_array[3] = 300; N_array[4] = 500; N_array[5] = 3;
		N_array[6] = 7; N_array[7] = 15;
		
		Integer [] same_coll_num_top_n = new Integer[feature_num];
		Double [] max_coll_score_top_n = new Double[feature_num];
		Double [] min_coll_score_top_n = new Double[feature_num];
		Double [] mean_coll_score_top_n = new Double[feature_num];
		for(int ii = 0; ii < feature_num; ii++){
			same_coll_num_top_n[ii] = 0;
			max_coll_score_top_n[ii] = 0.0;
			min_coll_score_top_n[ii] = 0.0;
			mean_coll_score_top_n[ii] = 0.0;
		}
		
		for(int ii = 0; ii < feature_num; ii++){
			int src_len_top_n = Math.min(src_len, N_array[ii]);
			int dest_len_top_n = Math.min(dest_len, N_array[ii]);
			for(int mm = 0; mm < src_len_top_n; mm++){
				for(int nn = 0; nn < dest_len_top_n; nn++){
					if(src_coll_id[mm] == dest_coll_id[nn]){
						same_coll_num_top_n[ii] ++;
						double sim_coll_score = src_coll_score[mm] + dest_coll_score[nn];
						mean_coll_score_top_n[ii] += sim_coll_score;
						if(sim_coll_score > max_coll_score_top_n[ii]){
							max_coll_score_top_n[ii] = sim_coll_score;
						}
						if(min_coll_score_top_n[ii] == 0){
							min_coll_score_top_n[ii] = sim_coll_score;
						} else if(sim_coll_score < min_coll_score_top_n[ii]){
							min_coll_score_top_n[ii] = sim_coll_score;
						}
					}
				}
			}
			if(same_coll_num_top_n[ii] > 0){
				mean_coll_score_top_n[ii] /= same_coll_num_top_n[ii];
			}		
		}
		
		String sim_coll_feature = "";
		for(int ii = 0; ii < feature_num; ii++){
			sim_coll_feature += ",sim_coll_top" + Integer.toString(N_array[ii]) + "_num:" + Integer.toString(same_coll_num_top_n[ii]) +
					  			",sim_coll_top" + Integer.toString(N_array[ii]) + "_max_score:" + Double.toString(max_coll_score_top_n[ii]) +
					  			",sim_coll_top" + Integer.toString(N_array[ii]) + "_min_score:" + Double.toString(min_coll_score_top_n[ii]) +
					  			",sim_coll_top" + Integer.toString(N_array[ii]) + "_mean_score:" + Double.toString(mean_coll_score_top_n[ii]);
		}
		sim_coll_feature = sim_coll_feature.substring(1);
		
		result.set(0,src_item_id);
		result.set(1,dest_item_id);
		result.set(2,sim_coll_feature);
		context.write(result);
	}
}