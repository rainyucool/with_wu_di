package word_match_probab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;

public class wordMatchProbabReducer extends ReducerBase {
	private Record result = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		result = context.createOutputRecord();
	}
	
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		long word_count_label0 = 0l;
		long word_count_label1 = 0l;
		while (values.hasNext()) {
			Record val = values.next();
			double label = Double.parseDouble(val.get(0).toString());
			if(label>0.5){
				word_count_label1 += 1;
			} else{
				word_count_label0 += 1;
			}
		}
		double word_match_probab = (double)word_count_label1/((double)word_count_label1+(double)word_count_label0);
		result.set(0,key.get(0));
		result.set(1,word_match_probab);
		context.write(result);
	}
}