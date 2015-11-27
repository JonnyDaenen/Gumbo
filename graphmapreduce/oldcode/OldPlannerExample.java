/**
 * Created: 12 May 2014
 */
package mapreduce.guardedfragment.planner;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.DepthPartitioner;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.compiler.resolver.CalculationCompiler;
import gumbo.compiler.resolver.MRPlan;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

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
		CULinker converter = new CULinker();
		CalculationUnitGroup calcUnits = converter.createDAG(gfe);
		System.out.println("\nCalculation Units\n-----------------");
		System.out.println(calcUnits);
		
		// partition
//		CalculationPartitioner partitioner = new HeightPartitioner();
		CalculationPartitioner partitioner = new DepthPartitioner();
//		CalculationPartitioner partitioner = new UnitPartitioner();
		PartitionedCUGroup partitionedUnits = partitioner.partition(calcUnits);
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
