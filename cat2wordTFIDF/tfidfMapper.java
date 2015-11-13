package cat2wordTFIDF;

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

public class tfidfMapper extends MapperBase {
	private Record key_cat_id;
	private Record val_item_title;
	private HashMap<Long,Long> item2cat_tab;

	@Override
	public void setup(TaskContext context) throws IOException { 
        key_cat_id = context.createMapOutputKeyRecord();
		val_item_title = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long item_id = Long.parseLong(record.get(0).toString());
		Long cat_id = Long.parseLong(record.get(1).toString());
		String item_title = record.get(2).toString();
		key_cat_id.set(new Object[]{cat_id});
		val_item_title.set(new Object[]{item_title});
		context.write(key_cat_id,val_item_title);
	}
}