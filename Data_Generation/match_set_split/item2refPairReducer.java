package Data_Generation.match_set_split;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class item2refPairReducer extends ReducerBase {
	private Record result;
	private HashMap<Long,Long> item2cat_tab;
	private HashMap<Long,String> item2title_tab;
	private HashMap<Long,Long> item2collId_tab;
	private HashMap<Long,HashMap<String,Double>> cat_word_tfidf_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
		item2cat_tab = new HashMap<Long,Long> ();
		item2title_tab = new HashMap<Long,String> ();
		item2collId_tab = new HashMap<Long,Long> ();
		Iterator<Record> rs_i2ct =context.readResourceTable("item_info_list_ref");
        while (rs_i2ct.hasNext()) {
            Record val = rs_i2ct.next();
            Long item_id = Long.parseLong(val.get(0).toString());
            Long cat_id = Long.parseLong(val.get(1).toString());
            String item_title = val.get(2).toString();
            item_title = rmv_dup_string(item_title);
            Long coll_id = Long.parseLong(val.get(3).toString());
            item2cat_tab.put(item_id, cat_id);
            item2title_tab.put(item_id, item_title);
            item2collId_tab.put(item_id, coll_id);
        }
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
					sum_tfidf_score += src_tfidf*dest_tfidf;
				}
			}
		}
		return sum_tfidf_score;
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		long dest_item_id = Long.parseLong(key.get(0).toString());
		String tmp_item_info = null;
		while(values.hasNext()){
			tmp_item_info = values.next().get(0).toString();
		}		
		String [] dest_item_info = tmp_item_info.split(":");
		if(dest_item_info.length==2){
			long dest_cat_id = Long.parseLong(dest_item_info[0]);
			String dest_item_title = dest_item_info[1];
			dest_item_title = rmv_dup_string(dest_item_title);
			
			Iterator<Long> item_iter = item2cat_tab.keySet().iterator();
			while(item_iter.hasNext()){
				long src_item_id = item_iter.next();
				long src_cat_id = item2cat_tab.get(src_item_id);
				String src_item_title = item2title_tab.get(src_item_id);
				long src_coll_id = item2collId_tab.get(src_item_id);
				
				if(src_cat_id != dest_cat_id){
					continue;
				}

				double coll_sim_score = get_tfidf_score(src_item_title, dest_item_title, src_cat_id, dest_cat_id);
				result.set(0,dest_item_id);
				result.set(1,src_coll_id);
				result.set(2,coll_sim_score);
				context.write(result);
			}
		}
	}
}