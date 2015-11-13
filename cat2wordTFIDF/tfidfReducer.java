package cat2wordTFIDF;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class tfidfReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		HashMap<String,Long> word_count_tab = new HashMap<String,Long> ();
		long total_num = 0;
		while (values.hasNext()) {
			total_num ++;
			Record val = values.next();
			String item_title = val.get(0).toString();
			String [] word_in_title = item_title.split(",");
			// remove duplicates
			HashSet<String> word_set = new HashSet<String>();
			for(int ii = 0; ii < word_in_title.length; ii++){
				word_set.add(word_in_title[ii]);
			}
			// word_count
			Iterator<String> word_iter= word_set.iterator();
			while(word_iter.hasNext()){			
				String word = word_iter.next();
				if(word_count_tab.containsKey(word)){
					word_count_tab.put(word,word_count_tab.get(word) + 1);
				}else{
					word_count_tab.put(word,1l);
				}
			}
		}
		Iterator<String> word_iter = word_count_tab.keySet().iterator();
		while(word_iter.hasNext()){
			String word = word_iter.next();
			double tf_idf_score = Math.log((double)total_num/(double)word_count_tab.get(word));
			result.set(0, key.get(0));
			result.set(1, word);
			result.set(2, tf_idf_score);
			context.write(result);
		}
	}
}