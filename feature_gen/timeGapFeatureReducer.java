package feature_gen;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

public class timeGapFeatureReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		double min_gap_time = 10000000;
		double max_gap_time = 0;
		double mean_gap_time = 0;
		double total_count = 0;
		double total_count_0_day = 0;
		double total_count_3_day = 0;
		double total_count_7_day = 0;
		double total_count_15_day = 0;
		
		ArrayList<Double> gap_time_list = new ArrayList<Double> ();
		while (values.hasNext()) {
			Record val = values.next();
			
			double tmp_gap_time = (double)val.getBigint(0);
			gap_time_list.add(tmp_gap_time);
			
			if(tmp_gap_time < min_gap_time){
				min_gap_time = tmp_gap_time;
			}
			if(tmp_gap_time > max_gap_time){
				max_gap_time = tmp_gap_time;
			}
			mean_gap_time += tmp_gap_time;
			
			total_count ++;
			if(tmp_gap_time == 0){total_count_0_day++;}
			else if(tmp_gap_time <= 3){total_count_3_day++;}
			else if(tmp_gap_time <= 7){total_count_7_day++;}
			else if(tmp_gap_time <= 15){total_count_15_day++;}
		}
		mean_gap_time /= total_count;

		double[] gap_time_arr = new double [(int)total_count];	
		// compute std
		int idx = 0;
		Iterator<Double> gap_iter = gap_time_list.iterator();
		double gap_time_std = 0;
		while(gap_iter.hasNext()){
			double tmp_gap_time = gap_iter.next();
			gap_time_std += Math.pow((tmp_gap_time-mean_gap_time),2);
			gap_time_arr[idx++] = tmp_gap_time;
		}
		gap_time_std = Math.sqrt(gap_time_std/total_count);
		
		// compute median
		Arrays.sort(gap_time_arr);
		double median_gap_time = gap_time_arr[(int)total_count/2];
		
		String feature_str = "min_gap_time" + ":" + Double.toString(min_gap_time) + "," +
							 "max_gap_time" + ":" + Double.toString(max_gap_time) + "," +
							 "mean_gap_time" + ":" + Double.toString(mean_gap_time) + "," +
							 "median_gap_time" + ":" + Double.toString(median_gap_time) + "," +
							 "gap_time_std" + ":" + Double.toString(gap_time_std) + "," +
							 "total_count" + ":" + Double.toString(total_count) + "," +
							 "count_0_day" + ":" + Double.toString(total_count_0_day) + "," +
							 "count_3_day" + ":" + Double.toString(total_count_3_day) + "," +
							 "count_7_day" + ":" + Double.toString(total_count_7_day) + "," +
							 "count_15_day" + ":" + Double.toString(total_count_15_day) + "," +
							 "ratio_0_day" + ":" + Double.toString(total_count_0_day/total_count) + "," +
							 "ratio_3_day" + ":" + Double.toString(total_count_3_day/total_count) + "," +
							 "ratio_7_day" + ":" + Double.toString(total_count_7_day/total_count) + "," +
							 "ratio_15_day" + ":" + Double.toString(total_count_15_day/total_count);
		
		String [] item_pair = key.get(0).toString().split(":");
		long item_id1 = Long.parseLong(item_pair[0]);
		long item_id2 = Long.parseLong(item_pair[1]);
		
		result.set(0,item_id1);
		result.set(1,item_id2);
		result.set(2,feature_str);		
		context.write(result);
 	}
}