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

public class item2matchIdMapper extends MapperBase {
	private Record key_item_id;
	private Record val_coll_info;
	private HashMap<Long,Long> item2cat_tab;
	private HashMap<Long,String> item2title_tab;
	private HashMap<Long,Long> item2collId_tab;
	private HashMap<Long,HashMap<String,Double>> cat_word_tfidf_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
        key_item_id = context.createMapOutputKeyRecord();
		val_coll_info = context.createMapOutputValueRecord();
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
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		long dest_item_id = Long.parseLong(record.get(0).toString());
		long ref_coll_id = Long.parseLong(record.get(1).toString());
		double coll_sim_score = Double.parseDouble(record.get(2).toString());

		String coll_info = Long.toString(ref_coll_id) + ":" + Double.toString(coll_sim_score);
		key_item_id.set(new Object[] { dest_item_id });
		val_coll_info.set(new Object[] { coll_info });
		context.write(key_item_id, val_coll_info);
	}
}