/**
 * Created: 27 Mar 2014
 */
package guardedfragment.mapreduce;

import guardedfragment.structure.*;
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
		GFAtomicExpression a3 = new GFAtomicExpression("S","x","y");
		GFOrExpression a4 = new GFOrExpression(a2,a3);
		GFNotExpression a5 = new GFNotExpression(a2);

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a5, "x");

		GFMRPlanner planner = new GFMRPlanner("./input/dummyrelations1", "./output/"
				+ GFMRlevel1Example.class.getName(), "./scratch/"  + GFMRlevel1Example.class.getSimpleName());


		try {
			MRPlan plan = planner.convert(e1);
			System.out.println(plan);
			System.out.println("AFTER PLAN");
			plan.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		LOG.info("Done.");


	}

}
