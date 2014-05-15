/**
 * Created: 15 May 2014
 */
package guardedfragment.mapreduce.planner;

import java.util.Collection;
import java.util.HashSet;

import mapreduce.MRPlan;

import org.apache.hadoop.fs.Path;

import guardedfragment.mapreduce.planner.partitioner.HeightPartitioner;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.io.DeserializeException;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

/**
 * @author Jonny Daenen
 * 
 */
public class NewPlannerExample {

	public static void main(String[] args) {
		try {
			
			// create directories
			long id = System.currentTimeMillis();
			Path indir = new Path("input/PlannerExample/");
			Path outdir = new Path("output/PlannerExample/" + id);
			Path scratchdir = new Path("scratch/PlannerExample/" + id);

			// create expressions
			Collection<GFExistentialExpression> expressions = loadExpressions();
			
			// create plan
			GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
			MRPlan plan = planner.createPlan(expressions, indir, outdir, scratchdir);
			
			// execute plan
			//plan.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * @return
	 * @throws DeserializeException 
	 */
	private static Collection<GFExistentialExpression> loadExpressions() throws DeserializeException {

		// raw expression
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		GFExistentialExpression gfe = (GFExistentialExpression) serializer.deserialize("#E(x)&A(x,y)&#C(x)&A(x,y)B(x)#D(x)&A(x,y)#F(x,y)&A(x,y)A(x,x)");

		HashSet<GFExistentialExpression> expressions = new HashSet<GFExistentialExpression>();
		expressions.add(gfe);

		return expressions;
	}
}
