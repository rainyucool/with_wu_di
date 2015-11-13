package feature_gen;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class rsl_correctMapper extends MapperBase {
	private Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		// ID = context.createMapOutputKeyRecord();
		// val = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String [] rsl = record.getString(1).split(",");
		String final_rsl = "";
		for(int ii = 0; ii < rsl.length; ii++){
			String tmp_rsl = rsl[ii].split("=")[0];
			final_rsl += ",";
			final_rsl += tmp_rsl;
		}
		final_rsl = final_rsl.substring(1);
		
		result = context.createOutputRecord();
		result.set(0, record.get(0));
		result.set(1, final_rsl);
		context.write(result);
	}
}