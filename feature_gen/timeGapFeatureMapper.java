package feature_gen;

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

public class timeGapFeatureMapper extends MapperBase {
	private Record key_item_pair;
	private Record val_gap_time;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load tfidf table
		key_item_pair = context.createMapOutputKeyRecord();
		val_gap_time = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String item_id1 = record.get(0).toString();
		String item_id2 = record.get(1).toString();
		Long gap_time = record.getBigint(2);
		
		val_gap_time.set(new Object[]{gap_time});
		
		String item_pair = item_id1 + ":" + item_id2;
		key_item_pair.set(new Object[]{item_pair});
		context.write(key_item_pair,val_gap_time);

		item_pair = item_id2 + ":" + item_id1;
		key_item_pair.set(new Object[]{item_pair});
		context.write(key_item_pair,val_gap_time);
	}
}