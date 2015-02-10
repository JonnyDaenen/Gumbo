/**
 * Created: 15 May 2014
 */
package mapreduce.guardedfragment.planner;

import gumbo.compiler.GFMRPlanner;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.compiler.structures.MRPlan;
import gumbo.engine.hadoop.HadoopExecutor;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.DeserializeException;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class OldPlannerExample2 {

	public static void main(String[] args) {
		try {
			
			// create directories
			long id = System.currentTimeMillis();
			Path indir = new Path("input/PlannerExample/");
			Path outdir = new Path("output/PlannerExample/" + id);
			Path scratchdir = new Path("scratch/PlannerExample/" + id);
			
			RelationFileMapping rfm = new RelationFileMapping();
			rfm.setDefaultPath(indir);

			// create expressions
			Collection<GFExistentialExpression> expressions = loadExpressions();
			
			
			
			// create plan
			GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
			MRPlan plan = planner.createPlan(expressions, rfm, outdir, scratchdir);
			
			// execute plan
			HadoopExecutor executor = new HadoopExecutor();
			executor.execute(plan);

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
