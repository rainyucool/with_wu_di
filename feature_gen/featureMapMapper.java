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

public class featureMapMapper extends MapperBase {
	private Record key_feature_name;
	private Record val_feature_val;

	@Override
	public void setup(TaskContext context) throws IOException { 
        key_feature_name = context.createMapOutputKeyRecord();
		val_feature_val = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String [] feature_info = record.get(2).toString().split(",");
		for(int ii = 0; ii < feature_info.length; ii++){
			String feature_name = feature_info[ii].split(":")[0];
			double feature_val = Double.parseDouble(feature_info[ii].split(":")[1]);
			key_feature_name.set(new Object[]{feature_name});
			val_feature_val.set(new Object[]{feature_val});
			context.write(key_feature_name,val_feature_val);
		}
	}
}