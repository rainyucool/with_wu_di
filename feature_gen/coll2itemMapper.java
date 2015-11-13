package feature_gen;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class coll2itemMapper extends MapperBase {
	private Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		// ID = context.createMapOutputKeyRecord();
		// val = context.createMapOutputValueRecord();
		result = context.createOutputRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		Long coll_id = Long.parseLong(record.get(0).toString());
		String [] item_list = record.getString(1).split(",|\\;");

		for(int ii = 0; ii < item_list.length; ii++){
			Long tmp_item_id = Long.parseLong(item_list[ii]);
			result.set(0,tmp_item_id);
			result.set(1,coll_id);
			context.write(result);
		}
	}
}