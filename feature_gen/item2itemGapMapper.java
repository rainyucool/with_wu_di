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

public class item2itemGapMapper extends MapperBase {
	private Record key_user_id;
	private Record val_item_info;

	@Override
	public void setup(TaskContext context) throws IOException {
		// load tfidf table
		key_user_id = context.createMapOutputKeyRecord();
		val_item_info = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long user_id = record.getBigint(0);
		Long item_id = record.getBigint(1);
		String date_time = record.getString(2);
		String item_info = Long.toString(item_id) + ":" + date_time;
		key_user_id.set(new Object[]{user_id});
		val_item_info.set(new Object[]{item_info});
		context.write(key_user_id,val_item_info);
	}
}