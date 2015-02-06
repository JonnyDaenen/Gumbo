/**
 * Created: 23 Jul 2014
 */
package gumbo;

import gumbo.compiler.GFMRPlanner;
import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.compiler.structures.MRPlan;
import gumbo.compiler.structures.RelationFileMapping;
import gumbo.executor.hadoop.HadoopExecutor;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.DeserializeException;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;

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
			

			RelationFileMapping rfm = new RelationFileMapping();
			rfm.setDefaultPath(new Path(input));
			MRPlan plan = planner.createPlan((GFExistentialExpression)gfe, rfm, new Path(output), new Path(scratch));
			//plan.execute();
			
			HadoopExecutor executor = new HadoopExecutor();
			executor.execute(plan);
			
//			SparkExecutor executor = new SparkExecutor();
//			executor.execute(plan);
			
		} catch (DeserializeException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		
		
		
	}
	
}
