package Data_Generation.match_set_split;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;

public class set2pairMapper extends MapperBase {
	private Record result;

	@Override
	public void setup(TaskContext context) throws IOException {
		// ID = context.createMapOutputKeyRecord();
		// val = context.createMapOutputValueRecord();
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String match_set = record.get(1).toString();
		String [] cat_set = match_set.split(";");
		for(int ii = 0; ii < cat_set.length; ii++){
			for(int jj = ii+1; jj < cat_set.length; jj++){
				String [] item_set1 = cat_set[ii].split(",");
				String [] item_set2 = cat_set[jj].split(",");
				for(int mm = 0; mm < item_set1.length; mm++){
					for(int nn = 0; nn < item_set2.length; nn++){
						result = context.createOutputRecord();
						result.set(0, Integer.parseInt(item_set1[mm]));
						result.set(1, Integer.parseInt(item_set2[nn]));
						result.set(2, 1);
						context.write(result);
					}
				}
			}
		}
	}
}