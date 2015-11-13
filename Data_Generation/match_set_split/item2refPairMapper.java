package Data_Generation.match_set_split;

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

public class item2refPairMapper extends MapperBase {
	private Record key_item_id;
	private Record val_item_info;
	private Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		key_item_id = context.createMapOutputKeyRecord();
		val_item_info = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		long dest_item_id = Long.parseLong(record.get(0).toString());
		long dest_cat_id = Long.parseLong(record.get(1).toString());
		String dest_item_title = record.get(2).toString();
		String dest_item_info = Long.toString(dest_cat_id) + ":" + dest_item_title;
		key_item_id.set(new Object[]{dest_item_id});
		val_item_info.set(new Object[]{dest_item_info});
		context.write(key_item_id,val_item_info);
	}
}