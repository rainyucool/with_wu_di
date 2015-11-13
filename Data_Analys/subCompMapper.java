package Data_Analys;

import java.io.IOException;
import java.util.Arrays;
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

public class subCompMapper extends MapperBase {
	private Record key_id;
	private Record val_id;

	@Override
	public void setup(TaskContext context) throws IOException {    
        key_id = context.createMapOutputKeyRecord();
		val_id = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String [] item_set1 = record.get(0).toString().split(",");
		String [] item_set2 = record.get(1).toString().split(",");
		int same_count = 0;
		for(int ii = 0; ii < item_set1.length; ii++){
			for(int jj = 0; jj < item_set2.length; jj++){
				if(item_set1[ii].equals(item_set2[jj])){
					same_count ++;
				}
			}
		}
		
		key_id.set(new Object[]{1});
		val_id.set(new Object[]{same_count});
		context.write(key_id,val_id);
	}
}