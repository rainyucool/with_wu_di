package word_match_probab;

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

public class wordMatchProbabMapper extends MapperBase {
	private Record key_word_id;
	private Record val_word_count;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load tfidf table
		key_word_id = context.createMapOutputKeyRecord();
		val_word_count = context.createMapOutputValueRecord();
	}
	
	public String get_same_word(String src_item_title,String dest_item_title,long src_cat_id,long dest_cat_id){
		String [] src_item_words = src_item_title.split(",");
		String [] dest_item_words = dest_item_title.split(",");
		String same_word_str = "";
		for(int ii = 0; ii < src_item_words.length; ii++){
			for(int jj = 0; jj < dest_item_words.length; jj++){
				String src_word = src_item_words[ii];
				String dest_word = dest_item_words[jj];
				if(src_word.equals(dest_word)){
					same_word_str += ';';
					same_word_str += src_word;
				}
			}
		}
		if(!same_word_str.equals("")){
			same_word_str = same_word_str.substring(1);
		}
		return same_word_str;
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		long src_item_id = record.getBigint(0);
		long src_cat_id = record.getBigint(1);
		String src_item_title = record.getString(2);
		long dest_item_id = record.getBigint(3);
		long dest_cat_id = record.getBigint(4);
		String dest_item_title = record.getString(5);
		long label = record.getBigint(6);
		
		String same_word_str = get_same_word(src_item_title,dest_item_title,src_cat_id,dest_cat_id);
		if(!same_word_str.equals("")){
			String [] same_word_list = same_word_str.split(";");
			for(int ii = 0; ii < same_word_list.length; ii++){
				String tmp_same_word = same_word_list[ii];
				key_word_id.set(new Object[]{tmp_same_word});
				val_word_count.set(new Object[]{label});
				context.write(key_word_id,val_word_count);
			}
		}		
	}
}