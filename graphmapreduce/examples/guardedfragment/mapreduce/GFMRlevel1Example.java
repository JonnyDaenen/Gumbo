/**
 * Created: 27 Mar 2014
 */
package guardedfragment.mapreduce;

import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFExistentialExpression;
import mapreduce.MRPlan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMRlevel1Example {
	
	private static final Log LOG = LogFactory.getLog(GFMRlevel1Example.class);

	public static void main(String[] args) {

		GFAtomicExpression a1 = new GFAtomicExpression("R", "x", "y", "x");
		GFAtomicExpression a2 = new GFAtomicExpression("S", "y", "x");

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a2, "x");

		GFMRPlanner planner = new GFMRPlanner("./input/dummyrelations1", "./output/"
				+ GFMRlevel1Example.class.getName(), "./scratch/"  + GFMRlevel1Example.class.getSimpleName());


		try {
			MRPlan plan = planner.convert(e1);
			System.out.println(plan);
			plan.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		LOG.info("Done.");


	}

}
