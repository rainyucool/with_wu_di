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

public class catClusterMapper extends MapperBase {
	private Record key_cat_set;
	private Record val_cat_set;

	@Override
	public void setup(TaskContext context) throws IOException {    
        key_cat_set = context.createMapOutputKeyRecord();
		val_cat_set = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String cat_id = record.get(0).toString();
		String cat_set = record.get(1).toString();
		cat_set = cat_set + ";" + cat_id;
		String [] cat_ids = cat_set.split(";");
		HashSet<String> cat_hash_set = new HashSet<String> ();
		
		for(int ii = 0; ii < cat_ids.length; ii++){
			cat_hash_set.add(cat_ids[ii]);
		}
		Integer [] cat_id_arr = new Integer [cat_hash_set.size()];
		Iterator<String> cat_hash_iter = cat_hash_set.iterator();
		int count = 0;
		while(cat_hash_iter.hasNext()){
			cat_id_arr[count++] = Integer.parseInt(cat_hash_iter.next());
		}
		
		Arrays.sort(cat_id_arr);
		String new_cat_id = "";
		for(int ii = 0; ii < cat_id_arr.length; ii++){
			new_cat_id += ";" + Integer.toString(cat_id_arr[ii]);
		}
		new_cat_id = new_cat_id.substring(1);
		
		key_cat_set.set(new Object[]{1});
		val_cat_set.set(new Object[]{new_cat_id});
		context.write(key_cat_set,val_cat_set);
	}
}