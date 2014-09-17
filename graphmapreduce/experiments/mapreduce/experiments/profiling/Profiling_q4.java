/**
 * Created: 10 Sep 2014
 */
package mapreduce.experiments.profiling;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.executor.spark.SparkExecutor;
import mapreduce.guardedfragment.planner.GFMRPlanner;
import mapreduce.guardedfragment.planner.GFMRPlannerException;
import mapreduce.guardedfragment.planner.PlannerExample;
import mapreduce.guardedfragment.planner.partitioner.UnitPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class Profiling_q4 {

	public static void main(String[] args) throws DeserializeException, GFMRPlannerException, IllegalArgumentException, InterruptedException {

		
//		ClassLoader cl = ClassLoader.getSystemClassLoader();
//		 
//        URL[] urls = ((URLClassLoader)cl).getURLs();
// 
//        for(URL url: urls){
//        	System.out.println(url.getFile());
//        }
		
		if (args.length == 0) {
			System.out.println("Please provide a input pattern as argument");
			System.exit(0);
		}
		
//		Thread.sleep(15000);
		
		
		
		// files & folders
		String input = args[0]; // "./input/q4/1e04/*.rel";
		String output = "./output/" + Profiling_q4.class.getSimpleName() + "/" + System.currentTimeMillis();
		String scratch = "./scratch/" + Profiling_q4.class.getSimpleName() + "/" + System.currentTimeMillis();

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
		MRPlan plan = planner.createPlan(gfes, new Path(input), new Path(output), new Path(scratch));

		// print plan in text
//		System.out.println(plan);

		// print plan in dot
//		System.out.println(plan.toDot());

		// execute plan
		long startTime = System.nanoTime();
		
		HadoopExecutor hExecutor = new HadoopExecutor();
		hExecutor.execute(plan);
		// SparkExecutor sExecutor = new SparkExecutor();
		// sExecutor.execute(plan);
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime)/1000000;
		System.out.println(duration);
		
	}

}
