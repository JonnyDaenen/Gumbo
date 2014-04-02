/**
 * Created: 27 Mar 2014
 */
package guardedfragment.mapreduce;

import guardedfragment.structure.GFAndExpression;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFNotExpression;
import guardedfragment.structure.GFOrExpression;
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
		GFAtomicExpression a2 = new GFAtomicExpression("S", "y", "y");
		GFAtomicExpression a3 = new GFAtomicExpression("S","x","x");
		GFAndExpression a4 = new GFAndExpression(a2,a3);
		GFAtomicExpression a6 = new GFAtomicExpression("T","x");
		GFOrExpression a7 = new GFOrExpression(a4,a6);
		GFNotExpression a5 = new GFNotExpression(a2);

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a7, "x");

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
