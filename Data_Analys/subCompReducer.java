package Data_Analys;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class subCompReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		double mean_same_count = 0.0;
		double max_same_count = 0.0;
		double min_same_count = 1000.0;
		int count = 0;
		while(values.hasNext()){
			count++;
			double tmp_same_count = Double.parseDouble(values.next().get(0).toString());
			mean_same_count = mean_same_count + tmp_same_count;
			if(tmp_same_count > max_same_count){
				max_same_count = tmp_same_count;
			}
			if(tmp_same_count < min_same_count){
				min_same_count = tmp_same_count;
			}
		}
		mean_same_count = mean_same_count/count;
		result.set(0,mean_same_count);
		result.set(1,max_same_count);
		result.set(2,min_same_count);
		
		context.write(result);
	}
}