package match_cat2cat;

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

public class cat2catMapper extends MapperBase {
	private Record key_cat_id;
	private Record val_cat_id;
	private HashMap<Long,Long> item2cat_tab;

	@Override
	public void setup(TaskContext context) throws IOException {
		item2cat_tab = new HashMap<Long,Long> ();		
		Iterator<Record> rs =context.readResourceTable("item2cat_tab");
        while (rs.hasNext()) {
            Record val = rs.next();
            Long item_id = Long.parseLong(val.get(0).toString());
            Long cat_id = Long.parseLong(val.get(1).toString());
            item2cat_tab.put(item_id, cat_id);
        }    
        key_cat_id = context.createMapOutputKeyRecord();
		val_cat_id = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String match_set = record.get(1).toString();
		String [] item_set_array = match_set.split(";");
		Long [] cat_id_array = new Long[item_set_array.length];
		for(int ii = 0; ii < item_set_array.length; ii++){
			long item_id = Long.parseLong(item_set_array[ii].split(",")[0]);
			long cat_id = item2cat_tab.get(item_id);
			cat_id_array[ii] = cat_id;
		}
		
		for(int ii = 0; ii < cat_id_array.length; ii++){
			for(int jj = 0; jj < cat_id_array.length; jj++){
				if(ii==jj){
					continue;
				}
				key_cat_id.set(new Object[]{cat_id_array[ii]});
				val_cat_id.set(new Object[]{cat_id_array[jj]});
				context.write(key_cat_id,val_cat_id);
			}
		}
	}
}