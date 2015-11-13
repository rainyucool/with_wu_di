package rsl_gen;

import java.io.IOException;
import java.util.HashMap;
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

public class rsl_tfidfMapper extends MapperBase {
	private Record key_item_id;
	private Record val_item_score;
	private HashMap<Long,HashMap<String,Double>> cat_word_tfidf_tab;
	private HashMap<String,Double> word_match_probab_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load tfidf table
		cat_word_tfidf_tab = new HashMap<Long, HashMap<String, Double>>();
		Iterator<Record> rs_tfidf = context.readResourceTable("word_tfidf_in_cat");
		while (rs_tfidf.hasNext()) {
			Record val = rs_tfidf.next();
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
		key_item_id = context.createMapOutputKeyRecord();
		val_item_score = context.createMapOutputValueRecord();
	}
	
	public double get_tfidf_score(String src_item_title,String dest_item_title,long src_cat_id,long dest_cat_id){
		String [] src_item_words = src_item_title.split(",");
		String [] dest_item_words = dest_item_title.split(",");
		double sum_tfidf_score = 0;
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
					sum_tfidf_score += (src_tfidf+dest_tfidf)*word_probab;
				}
			}
		}
		return sum_tfidf_score;
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long src_item_id = record.getBigint(0);
		Long src_cat_id = record.getBigint(1);
		String src_item_title = record.getString(2);
		Long dest_item_id = record.getBigint(3);
		Long dest_cat_id = record.getBigint(4);
		String dest_item_title = record.getString(5);
		
		double sim_tfidf = get_tfidf_score(src_item_title,dest_item_title,src_cat_id,dest_cat_id);
		if(sim_tfidf>0){
			String tmp_item_score = Long.toString(dest_item_id) + ":" + Double.toString(sim_tfidf);
			key_item_id.set(new Object[]{src_item_id});
			val_item_score.set(new Object[]{tmp_item_score});
			context.write(key_item_id,val_item_score);
		}
	}
}