/**
 * Created: 12 May 2014
 */
package mapreduce.guardedfragment.planner;

import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;
import mapreduce.guardedfragment.planner.calculations.GFtoCalculationUnitConverter;
import mapreduce.guardedfragment.planner.compiler.CalculationCompiler;
import mapreduce.guardedfragment.planner.partitioner.CalculationPartitioner;
import mapreduce.guardedfragment.planner.partitioner.DepthPartitioner;
import mapreduce.guardedfragment.planner.partitioner.PartitionedCalculationUnitDAG;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 *
 */
public class OldPlannerExample {

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
		
		// input files
		RelationFileMapping rfm = new RelationFileMapping();
		rfm.setDefaultPath(indir);
		
		
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
		MRPlan plan  = compiler.compile(partitionedUnits, rfm, outdir, scratchdir);
		System.out.println("\nMR-plan\n-------");
		System.out.println(plan);
		
		
		//plan.execute();
	}
	
}
