/**
 * Created: 10 Sep 2014
 */
package mapreduce.experiments;

import gumbo.compiler.GFCompilerException;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.compiler.resolver.MRPlan;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class Experiment_q4 {

	public static void main(String[] args) throws DeserializeException, GFCompilerException, IllegalArgumentException {

		if (args.length == 0) {
			System.out.println("Please provide a input pattern as argument");
			System.exit(0);
		}

		// files & folders
		String input = args[0]; // "./input/q4/1e04/*.rel";
		String output = "./output/" + Experiment_q4.class.getSimpleName() + "/" + System.currentTimeMillis();
		String scratch = "./scratch/" + Experiment_q4.class.getSimpleName() + "/" + System.currentTimeMillis();

		RelationFileMapping rfm = new RelationFileMapping();
		rfm.setDefaultPath(new Path(input));
		
		
		// query

		Set<String> queries = new HashSet<String>();
		queries.add("#Out(x2)&R(x2,x3,x4,x5)&!S2(x2)&!S3(x3)&!S4(x4)!S5(x5)");

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();

		Collection<GFExpression> gfes1 = parser.deserialize(queries);
		Collection<GFExistentialExpression> gfes = new HashSet<GFExistentialExpression>();
		for (GFExpression gfExpression : gfes1) {
			gfes.add((GFExistentialExpression) gfExpression);
		}

		// plan
		// GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
		GFMRPlanner planner = new GFMRPlanner(new UnitPartitioner());
		
		MRPlan plan = planner.createPlan(gfes, rfm, new Path(output), new Path(scratch));

		// print plan in text
		System.out.println(plan);

		// print plan in dot
		System.out.println(plan.toDot());

		// execute plan
		HadoopExecutor hExecutor = new HadoopExecutor();
		hExecutor.execute(plan);
		// SparkExecutor sExecutor = new SparkExecutor();
		// sExecutor.execute(plan);
	}

}
