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

public class rsl_xgboostMapper extends MapperBase {
	private Record key_item_id;
	private Record val_item_score;
	private HashMap<Long,HashMap<String,Double>> cat_word_tfidf_tab;
	private HashMap<String,Double> word_match_probab_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		key_item_id = context.createMapOutputKeyRecord();
		val_item_score = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long src_item_id = record.getBigint(0);
		Long dest_item_id = record.getBigint(1);
		Double probab = record.getDouble(2);
		
		String tmp_item_score = Long.toString(dest_item_id) + ":" + Double.toString(probab);
		key_item_id.set(new Object[] { src_item_id });
		val_item_score.set(new Object[] { tmp_item_score });
		context.write(key_item_id, val_item_score);
	}
}