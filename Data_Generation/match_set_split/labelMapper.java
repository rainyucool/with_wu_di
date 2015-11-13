package Data_Generation.match_set_split;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class labelMapper extends MapperBase {
	private Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		// ID = context.createMapOutputKeyRecord();
		// val = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
//		String rowID = record.get(0).;
//		String val = record.get(1).toString();

		double randNum = Math.random();
		String label = null;
		if (randNum < 0.5) {
			label = "ref";
		} else if (randNum < 0.9) {
			label = "train";
		} else {
			label = "test";
		}
		result = context.createOutputRecord();
		result.set(0, record.get(0));
		result.set(1, label);
		context.write(result);
	}
}