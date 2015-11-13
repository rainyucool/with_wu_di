package feature_gen;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class featureMapReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		double max_val = 0;
		double min_val = 10000000;
		double mean_val = 0;
		int count = 0;
		while (values.hasNext()) {
			count ++;
			double feature_val = Double.parseDouble(values.next().get(0).toString());
			if(feature_val > max_val){max_val = feature_val;}
			if(feature_val < min_val){min_val = feature_val;}
			mean_val += feature_val;		
		}
		mean_val /= count;
		result.set(0,key.get(0));
		result.set(1,max_val);
		result.set(2,min_val);
		result.set(3,mean_val);
		context.write(result);
	}
}