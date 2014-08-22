/**
 * Created: 23 Jul 2014
 */
package guardedfragment.mapreduce;

import guardedfragment.mapreduce.planner.partitioner.HeightPartitioner;
import guardedfragment.mapreduce.planner.structures.MRPlan;
import guardedfragment.mapreduce.planner.GFMRPlanner;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.DeserializeException;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
			plan.execute();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (DeserializeException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		
		
		
	}
	
}
