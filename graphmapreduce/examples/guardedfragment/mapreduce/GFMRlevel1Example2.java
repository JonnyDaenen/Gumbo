/**
 * Created: 27 Mar 2014
 */
package guardedfragment.mapreduce;

import guardedfragment.structure.GFAndExpression;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFNotExpression;
import mapreduce.MRPlan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMRlevel1Example2 {
	
	private static final Log LOG = LogFactory.getLog(GFMRlevel1Example2.class);

	public static void main(String[] args) {

		GFAtomicExpression a1 = new GFAtomicExpression("R", "x", "y", "z");
		GFAtomicExpression a2 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression a3 = new GFAtomicExpression("S","y","z");
		GFNotExpression a4 = new GFNotExpression(a3);
		GFAndExpression a5 = new GFAndExpression(a2,a4);

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a5, "x");

		GFMRPlanner planner = new GFMRPlanner("./input/dummyrelations2", "./output/"
				+ GFMRlevel1Example2.class.getName()+"/"+System.currentTimeMillis(), "./scratch/"  + GFMRlevel1Example2.class.getSimpleName()+"/"+System.currentTimeMillis());


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
