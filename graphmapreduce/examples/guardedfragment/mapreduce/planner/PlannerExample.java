/**
 * Created: 12 May 2014
 */
package guardedfragment.mapreduce.planner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;
import guardedfragment.mapreduce.planner.calculations.GFtoCalculationUnitConverter;
import guardedfragment.mapreduce.planner.compiler.CalculationCompiler;
import guardedfragment.mapreduce.planner.partitioner.CalculationPartitioner;
import guardedfragment.mapreduce.planner.partitioner.DepthPartitioner;
import guardedfragment.mapreduce.planner.partitioner.HeightPartitioner;
import guardedfragment.mapreduce.planner.partitioner.PartitionedCalculationUnitDAG;
import guardedfragment.mapreduce.planner.partitioner.UnitPartitioner;
import guardedfragment.mapreduce.planner.structures.MRPlan;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 *
 */
public class PlannerExample {

	public static void main(String[] args) throws Exception {
		
		long id = System.currentTimeMillis();
		Path indir = new Path("input/PlannerExample/");
		Path outdir = new Path("output/PlannerExample/"+id);
		Path scratchdir = new Path("scratch/PlannerExample/"+id);
	
		// raw expression
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		GFExistentialExpression gfe = (GFExistentialExpression) serializer.deserialize(
				"#E(x)&A(x,y)&#C(x)&A(x,y)B(x)#D(x)&A(x,y)A(x,x)"
				);
		System.out.println("\nGFE\n---");
		System.out.println(gfe);
		
		
		// convert to calculations
		GFtoCalculationUnitConverter converter = new GFtoCalculationUnitConverter();
		CalculationUnitDAG calcUnits = converter.createCalculationUnits(gfe);
		System.out.println("\nCalculation Units\n-----------------");
		System.out.println(calcUnits);
		
		// partition
//		CalculationPartitioner partitioner = new HeightPartitioner();
		CalculationPartitioner partitioner = new DepthPartitioner();
//		CalculationPartitioner partitioner = new UnitPartitioner();
		PartitionedCalculationUnitDAG partitionedUnits = partitioner.partition(calcUnits);
		System.out.println("\nPartitioned Units\n-----------------");
		System.out.println(partitionedUnits);
		
		// compile
		CalculationCompiler compiler = new CalculationCompiler();
		MRPlan plan  = compiler.compile(partitionedUnits, indir, outdir, scratchdir);
		System.out.println("\nMR-plan\n-------");
		System.out.println(plan);
		
		
		//plan.execute();
	}
	
}
