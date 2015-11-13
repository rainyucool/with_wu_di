package feature_gen;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class item2itemGapReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	public long get_time_gap(String src_date,String dest_date){
		long time_unit = 1000*60*60*24;
		Date DateII = null;
		Date DateJJ = null;
		try {
			DateII = new SimpleDateFormat("yyyyMMdd").parse(src_date);
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			DateJJ = new SimpleDateFormat("yyyyMMdd").parse(dest_date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long gap_time = Math.abs(DateII.getTime() - DateJJ.getTime())/time_unit;
		return gap_time;
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		HashMap<Long,ArrayList<String>> item_date = new HashMap<Long,ArrayList<String>> ();
		while (values.hasNext()) {
			Record val = values.next();
			String [] item_info = val.get(0).toString().split(":");
			long item_id = Long.parseLong(item_info[0]);
			String date_time = item_info[1];
			if(!item_date.containsKey(item_id)){
				ArrayList<String> date_list = new ArrayList<String> ();
				date_list.add(date_time);
				item_date.put(item_id, date_list);
			} else{
				ArrayList<String> date_list = item_date.get(item_id);
				date_list.add(date_time);
				item_date.put(item_id, date_list);
			}
		}
		// load item_ids into list
		Long [] item_id_list = new Long[item_date.size()];
		Iterator<Long> item_id_iter = item_date.keySet().iterator();
		int count = 0;
		while(item_id_iter.hasNext()){
			item_id_list[count] = item_id_iter.next();
			count++;
		}
		// calculate time gap
		for(int ii = 0; ii < item_id_list.length; ii++){
			for(int jj = ii+1; jj < item_id_list.length; jj++){
				long item_id_ii = item_id_list[ii];
				long item_id_jj = item_id_list[jj];
				ArrayList<String> date_list_ii = item_date.get(item_id_ii);
				ArrayList<String> date_list_jj = item_date.get(item_id_jj);
				long min_gap = 100000000l;
				Iterator<String> date_iter_ii = date_list_ii.iterator();
				while(date_iter_ii.hasNext()){
					String date_ii = date_iter_ii.next();
					Iterator<String> date_iter_jj = date_list_jj.iterator();
					while(date_iter_jj.hasNext()){
						String date_jj = date_iter_jj.next();
						long time_gap = get_time_gap(date_ii,date_jj);
						if(time_gap < min_gap){
							min_gap = time_gap;
						}
					}		
				}
				result.set(0,item_id_ii);
				result.set(1,item_id_jj);
				result.set(2,min_gap);
				context.write(result);
			}
		}	
	}
}