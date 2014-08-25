/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor.spark;

import mapreduce.guardedfragment.executor.GFExecutor;
import mapreduce.guardedfragment.planner.structures.MRPlan;

/**
 * Executes an MR-plan on Spark.
 * 
 * @author Jonny Daenen
 *
 */
public class SparkExecutor implements GFExecutor {
	
	public void execute(MRPlan plan){
		
		// for each partition, bottom to top TODO make iterable
		// TODO are the jobs merged already?
		
		// initialize
		
		// execute
		
		// cleanup
		
	}

}
