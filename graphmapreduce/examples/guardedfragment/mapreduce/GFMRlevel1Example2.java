/**
 * Created: 27 Mar 2014
 */
package guardedfragment.mapreduce;

import java.util.Set;

import guardedfragment.structure.DeserializeException;
import guardedfragment.structure.GFAndExpression;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFExpression;
import guardedfragment.structure.GFNotExpression;
import guardedfragment.structure.GFOrExpression;
import guardedfragment.structure.MyGFParser;
import mapreduce.MRPlan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMRlevel1Example2 {
	
	private static final Log LOG = LogFactory.getLog(GFMRlevel1Example2.class);

	public static void main(String[] args) throws DeserializeException {

		GFAtomicExpression a1 = new GFAtomicExpression("R", "x", "y", "z");
		GFAtomicExpression a2 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression a3 = new GFAtomicExpression("S","y","z");
		GFNotExpression a4 = new GFNotExpression(a3);
		GFAndExpression a5 = new GFAndExpression(a2,a4);

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a5, "OUTPUT","x");

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

		testing();

	}
	
	
	private static void testing() {
		GFAtomicExpression a1 = new GFAtomicExpression("R", "x", "y", "z");
		GFAtomicExpression a2 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression a3 = new GFAtomicExpression("S","y","z");
		GFNotExpression a4 = new GFNotExpression(a3);
		GFAndExpression a5 = new GFAndExpression(a2,a4);

		GFExistentialExpression e1 = new GFExistentialExpression(a1, a5, "OUTPUT","x");
		GFAndExpression f = new GFAndExpression(e1,e1);
		GFNotExpression g = new GFNotExpression(f);
		GFOrExpression h = new GFOrExpression(g,a5);


		
		GFExistentialExpression h1 = new GFExistentialExpression(a3,h, "Out2", "y");
		
		System.out.println("The rank of h1: "+h1.getRank());
		System.out.println(h1.generateString());
		
		Set<GFExistentialExpression> set = h1.getSubExistentialExpression(1);
		for (GFExistentialExpression s : set) {
		    System.out.println(s.generateString());
		}

		for (GFExistentialExpression s : set) {
		    System.out.println(s.generateString());
		}

		
	}

}
