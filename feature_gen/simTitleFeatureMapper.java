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

public class simTitleFeatureMapper extends MapperBase {
	private Record result = null;
	private HashMap<Long,HashMap<String,Double>> cat_word_tfidf_tab;
	private HashMap<String,Double> word_match_probab_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load tfidf table
		cat_word_tfidf_tab = new HashMap<Long, HashMap<String, Double>>();
		Iterator<Record> rs = context.readResourceTable("word_tfidf_in_cat");
		while (rs.hasNext()) {
			Record val = rs.next();
			Long cat_id = Long.parseLong(val.get(0).toString());
			String word_id = val.get(1).toString();
			Double tfidf = Double.parseDouble(val.get(2).toString());
			if (!cat_word_tfidf_tab.containsKey(cat_id)) {
				HashMap<String, Double> tmp_word_tfidf_tab = new HashMap<String, Double>();
				tmp_word_tfidf_tab.put(word_id, tfidf);
				cat_word_tfidf_tab.put(cat_id, tmp_word_tfidf_tab);
			} else {
				cat_word_tfidf_tab.get(cat_id).put(word_id, tfidf);
			}
		}
		// load word probab table
		word_match_probab_tab = new HashMap<String, Double>();
		Iterator<Record> rs_probab = context.readResourceTable("word_match_probab_tab");
		while (rs_probab.hasNext()) {
			Record val = rs_probab.next();
			String word_id = val.get(0).toString();
			Double probab = Double.parseDouble(val.get(1).toString());
			word_match_probab_tab.put(word_id, probab);
		}
	}
	
	public String get_tfidf_score(String src_item_title,String dest_item_title,long src_cat_id,long dest_cat_id){
		String [] src_item_words = src_item_title.split(",");
		String [] dest_item_words = dest_item_title.split(",");
		double sum_sim_score = 0;
		double same_word_num = 0;
		double sum_multi_tfidf = 0;
		double sum_word_probab = 0;
		double sum_diff_tfidf = 0;
		double sum_tfidf = 0;
		for(int ii = 0; ii < src_item_words.length; ii++){
			for(int jj = 0; jj < dest_item_words.length; jj++){
				String src_word = src_item_words[ii];
				String dest_word = dest_item_words[jj];
				if(src_word.equals(dest_word)){
					double src_tfidf = cat_word_tfidf_tab.get(src_cat_id).get(src_word);
					double dest_tfidf = cat_word_tfidf_tab.get(dest_cat_id).get(dest_word);
					double word_probab = 0.29;
					if(word_match_probab_tab.containsKey(src_word)){
						word_probab = word_match_probab_tab.get(src_word);
					}
					sum_sim_score += (src_tfidf+dest_tfidf)*word_probab;
					sum_multi_tfidf += src_tfidf*dest_tfidf;
					sum_word_probab += word_probab;
					sum_tfidf += src_tfidf + dest_tfidf;
					sum_diff_tfidf += Math.abs(src_tfidf-dest_tfidf);
					same_word_num ++;
				}
			}
		}
		//double ratio_diff_tfidf = sum_diff_tfidf/sum_tfidf;
		String sim_title_feature = "sim_score:" + Double.toString(sum_sim_score) + "," + 
								   "cos_tfidf_score:" + Double.toString(sum_multi_tfidf) + "," + 
								   "sum_tfidf_score:" + Double.toString(sum_tfidf) + "," + 
								   "sum_word_probab:" + Double.toString(sum_word_probab) + "," +
								   "num_same_word:" + Double.toString(same_word_num);
		return sim_title_feature;
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
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		Long src_item_id = record.getBigint(0);
		Long src_cat_id = record.getBigint(1);
		String src_item_title = record.getString(2);
		src_item_title = rmv_dup_string(src_item_title);
		Long dest_item_id = record.getBigint(3);
		Long dest_cat_id = record.getBigint(4);
		String dest_item_title = record.getString(5);
		dest_item_title = rmv_dup_string(dest_item_title);
		
		String sim_title_feature = get_tfidf_score(src_item_title, dest_item_title, src_cat_id, dest_cat_id);
		result.set(0,src_item_id);
		result.set(1,dest_item_id);
		result.set(2,sim_title_feature);
		context.write(result);
	}
}