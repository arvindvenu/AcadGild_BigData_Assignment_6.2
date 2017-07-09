package mapreduce.task9;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 
 * @author arvind
 * For this implementation we need a custom partitioner because 
 * there are multiple reducers and the requirement is to sort based on 
 * the number of units sold. If two key value pairs with different keys go 
 * to different reducers in a random order, then the final aggregated output 
 * from all the reducers will not be sorted. So we need to ensure that the 
 * lowest n keys need to go to the 1st reducer and the next lowest n keys 
 * to go to the 2nd reducer and so on. The highest n keys will have to go to 
 * the last reducer  
 *
 * This class implements Configurable because the configuration properties set in the driver 
 * class for this job needs to be available here for the partition to be done uniformly. The conf object
 * is automatically injected and provided to us by the hadoop framework
 */

public class Task9Partitioner extends Partitioner<IntWritable, Text> implements Configurable{

	private Configuration conf;
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int getPartition(IntWritable totalUnitsSold, Text companyName, int numReduceTasks) {
		
		// total units sold as the key output by the mapper
		int totUnitsSold = totalUnitsSold.get();
		
		// obtaining the minTotalUnitsSoldInput from the configuration
		int minTotalUnitsSoldInput = conf.getInt(Task9Constants.MIN_TOTAL_UNITS_SOLD_KEY, Task9Constants.MIN_DEFAULT_TOTAL_UNITS_SOLD);
		
		// obtaining the maxTotalUnitsSoldInput from the configuration2
		int maxUnitsSoldInput = conf.getInt(Task9Constants.MAX_TOTAL_UNITS_SOLD_KEY,Task9Constants.MAX_DEFAULT_TOTAL_UNITS_SOLD);
		
		/* assuming the totalUnitsSold is uniformly distributed, calculating the size of each partition
		 * (the number of keys that will go to a particular reducer)
		*/
		int partitionSize = (maxUnitsSoldInput - minTotalUnitsSoldInput)/numReduceTasks;
		
		/*
		 * Calculating the partition number that this key value will fall in
		 */
		int partitionNumber = (totUnitsSold - minTotalUnitsSoldInput) / partitionSize;
		
		/* Safety check. If the partition number calculated above happens to be negative,
		 * route it to the first reducer. If the partition number happens to be not less than 
		 * the number of reducers assign it to the last reducer. This should ideally not happen 
		 * because the  map phase already filters out those records which are not within the min and max  
		 */
		
		if(partitionNumber < 0 ) {
			partitionNumber = 0;
		} else if(partitionNumber >= numReduceTasks) {
			partitionNumber = numReduceTasks - 1;
		}
		return partitionNumber;
	}

}
