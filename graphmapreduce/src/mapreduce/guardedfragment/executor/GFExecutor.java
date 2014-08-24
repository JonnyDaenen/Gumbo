/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor;

import mapreduce.guardedfragment.planner.structures.MRPlan;

/**
 * @author jonny
 *
 */
public interface GFExecutor {

	public void execute(MRPlan plan);
	
}
