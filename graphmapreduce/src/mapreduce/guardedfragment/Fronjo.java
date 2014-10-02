/**
 * Created: 23 Jul 2014
 */
package mapreduce.guardedfragment;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.planner.GFMRPlanner;
import mapreduce.guardedfragment.planner.partitioner.HeightPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 *
 * Main class for testing the Fronjo system.
 *
 */
public class Fronjo {

	public static void main(String[] args) {
		
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	
		
		
		try {
			// ask for input, output and scratch dir
//			System.out.print("Input folder: ");
//			String input = br.readLine();
//			
//			System.out.print("Output folder: ");
//			String output = br.readLine();
//			
//			System.out.print("Scratch folder: ");
//			String scratch = br.readLine();
//			
//			// ask for query
//			System.out.print("Query: ");
//			String query = br.readLine();
			
			
			if(args.length < 4)
				System.out.println("\tUsage: program inputfolder outputfolder scratchfolder query");
			
			String input = args[0];
			String output = args[1];
			String scratch = args[2];
			String query = args[3];
			
			// parse query
			GFPrefixSerializer parser = new GFPrefixSerializer();
			GFExpression gfe = parser.deserialize(query);
			
			// plan and execute query
			GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
			MRPlan plan = planner.createPlan((GFExistentialExpression)gfe, new Path(input), new Path(output), new Path(scratch));
			//plan.execute();
			
			HadoopExecutor executor = new HadoopExecutor();
			executor.execute(plan);
			
		} catch (DeserializeException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		
		
		
	}
	
}
