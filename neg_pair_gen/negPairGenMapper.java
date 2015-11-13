package neg_pair_gen;

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

public class negPairGenMapper extends MapperBase {
	private Record result;
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
	}
	
	public boolean check_if_mutual(String match_set0,String match_set1){
		String [] item_set_arr0 = match_set0.split(";");
		String [] item_set_arr1 = match_set1.split(";");
		if(item_set_arr0.length != item_set_arr1.length){
			return false;
		}
		// get cat set
		HashSet<Long> cat_set0 = new HashSet<Long> ();
		HashSet<Long> cat_set1 = new HashSet<Long> ();
		HashSet<Long> item_set0 = new HashSet<Long> ();
		HashSet<Long> item_set1 = new HashSet<Long> ();
		for(int ii = 0; ii < item_set_arr0.length; ii++){
			String [] tmp_item_set = item_set_arr0[ii].split(",");
			long tmp_item_id = Long.parseLong(tmp_item_set[0]);
			long tmp_cat_id = item2cat_tab.get(tmp_item_id);
			cat_set0.add(tmp_cat_id);
			for(int jj = 0; jj < tmp_item_set.length; jj++){
				long item_id = Long.parseLong(tmp_item_set[jj]);
				item_set0.add(item_id);
			}
		}
		for(int ii = 0; ii < item_set_arr1.length; ii++){
			String [] tmp_item_set = item_set_arr1[ii].split(",");
			long tmp_item_id = Long.parseLong(tmp_item_set[0]);
			long tmp_cat_id = item2cat_tab.get(tmp_item_id);
			cat_set1.add(tmp_cat_id);
			for(int jj = 0; jj < tmp_item_set.length; jj++){
				long item_id = Long.parseLong(tmp_item_set[jj]);
				item_set1.add(item_id);
			}
		}
		if(cat_set0.size() != cat_set1.size()){
			return false;
		}
		Iterator<Long> iter_cat_set0 = cat_set0.iterator();
		while(iter_cat_set0.hasNext()){
			long tmp_cat_id = iter_cat_set0.next();
			if(!cat_set1.contains(tmp_cat_id)){
				return false;
			}
		}
		Iterator<Long> iter_item_set0 = item_set0.iterator();
		while(iter_item_set0.hasNext()){
			long tmp_item_id = iter_item_set0.next();
			if(item_set1.contains(tmp_item_id)){
				return false;
			}
		}
		return true;
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		result = context.createOutputRecord();
		String match_set0 = record.get(0).toString();
		String match_set1 = record.get(1).toString();
		if(check_if_mutual(match_set0,match_set1)){
			String [] item_set_arr0 = match_set0.split(";");
			String [] item_set_arr1 = match_set1.split(";");
			for(int ii = 0; ii < item_set_arr0.length; ii++){
				for(int jj = 0; jj < item_set_arr1.length; jj++){
					String [] item_set0 = item_set_arr0[ii].split(",");
					String [] item_set1 = item_set_arr1[jj].split(",");
					long tmp_item_id0 = Long.parseLong(item_set0[0]);
					long tmp_item_id1 = Long.parseLong(item_set1[0]);
					if(item2cat_tab.get(tmp_item_id0) == item2cat_tab.get(tmp_item_id1)){
						continue;
					}
					for(int mm = 0; mm < item_set0.length; mm++){
						for(int nn = 0; nn < item_set1.length; nn++){
							result.set(0,Long.parseLong(item_set0[mm]));
							result.set(1,Long.parseLong(item_set1[nn]));
							result.set(2,0l);
							context.write(result);
						}
					}
				}
			}
		}
		
		
	}
}