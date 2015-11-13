package match_cat2cat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class cat2catReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		HashSet<Long> cat_id_set = new HashSet<Long>();
		while (values.hasNext()) {
			Record val = values.next();
			Long cat_id = Long.parseLong(val.get(0).toString());
			cat_id_set.add(cat_id);
		}
		String cat_id_str = "";
		Iterator<Long> cat_id_iter = cat_id_set.iterator();
		while(cat_id_iter.hasNext()){
			cat_id_str += ";";
			cat_id_str += cat_id_iter.next();
		}
		cat_id_str = cat_id_str.substring(1);
		result.set(0, key.get(0));
		result.set(1, cat_id_str);
		context.write(result);
	}
}