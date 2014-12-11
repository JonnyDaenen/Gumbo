/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.partitioner.OptimalPartitioner;
import mapreduce.guardedfragment.planner.partitioner.UnitPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class PlannerExample {

	public static void main(String[] args) throws Exception {
		
		// files & folders
		String input = "./input/q2/";
		String output = "./output/"+PlannerExample.class.getSimpleName()+"/"+System.currentTimeMillis();
		String scratch = "./scratch/"+PlannerExample.class.getSimpleName()+"/"+System.currentTimeMillis();
		

		RelationFileMapping rfm = new RelationFileMapping();
		rfm.setDefaultPath(new Path(input));
		rfm.addPath(new RelationSchema("R",2), new Path("./input/q2/text.txt"));
		rfm.addPath(new RelationSchema("S3",2), new Path("./input/q2/S3.txt"));
		
		// query 
		
		Set<String> queries = new HashSet<String>();
		queries.add("#O(x1,x2)&R(x1,x2)&!S1(x1)!S2(x2)");
		queries.add("#P(x1,x2)&R(x1,x2)O(x2,x2)");
		queries.add("#Q(x1,x2)&R(x1,x2)O(x1,x1)");
//		queries.add("#M(x1,x2)&S3(x1,x2)Q(x1,x1)");

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();
		
		Collection<GFExpression> gfes1 = parser.deserialize(queries);
		Collection<GFExistentialExpression> gfes = new HashSet<GFExistentialExpression>();
		for (GFExpression gfExpression : gfes1) {
			gfes.add((GFExistentialExpression) gfExpression);
		}
		
		// plan
//		GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
//		GFMRPlanner planner = new GFMRPlanner(new UnitPartitioner());
		GFMRPlanner planner = new GFMRPlanner(new OptimalPartitioner());
		MRPlan plan = planner.createPlan( gfes, rfm, new Path(output), new Path(
				scratch));
		
		// print plan in text
		System.out.println(plan);
		
		// print plan in dot
		System.out.println(plan.toDot());
		
		

		// execute plan
//		HadoopExecutor hExecutor = new HadoopExecutor();
//		SparkExecutor sExecutor = new SparkExecutor();
//		sExecutor.execute(plan);
		
	}

}
