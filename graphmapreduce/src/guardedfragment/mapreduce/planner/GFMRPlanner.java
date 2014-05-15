/**
 * Created: 15 May 2014
 */
package guardedfragment.mapreduce.planner;

import guardedfragment.mapreduce.planner.calculations.CalculationUnitDAG;
import guardedfragment.mapreduce.planner.calculations.GFtoCalculationUnitConverter;
import guardedfragment.mapreduce.planner.compiler.CalculationCompiler;
import guardedfragment.mapreduce.planner.partitioner.CalculationPartitioner;
import guardedfragment.mapreduce.planner.partitioner.PartitionedCalculationUnitDAG;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;

import java.util.Collection;

import mapreduce.MRPlan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMRPlanner {
	

	private static final Log LOG = LogFactory.getLog(GFMRPlanner.class); 

	private GFtoCalculationUnitConverter converter;
	private CalculationPartitioner partitioner;
	private CalculationCompiler compiler;

	/**
	 * 
	 */
	public GFMRPlanner(CalculationPartitioner partitioner) {
		this.partitioner = partitioner;
		
		converter = new GFtoCalculationUnitConverter();
		compiler = new CalculationCompiler();
	}

	/**
	 * Creates a MR-plan for a given set of existential expressions.
	 * 
	 * @param expressions the set of expressions to convert
	 * @param indir the directory containing the input data
	 * @param outdir the directory where to put the output data
	 * @param scratchdir the directory the plan can use
	 * 
	 * @return A MR-plan for calculation the GFE's
	 * 
	 * @throws GFMRPlannerException 
	 */
	public MRPlan createPlan(Collection<GFExistentialExpression> expressions, Path indir, Path outdir, Path scratchdir) throws GFMRPlannerException {


		// convert to calculations
		LOG.info("Converting GFE's to calculation units...");
		CalculationUnitDAG calcUnits = new CalculationUnitDAG();
		for (GFExistentialExpression gfe : expressions) {
			calcUnits.addAll(converter.createCalculationUnits(gfe));
		}
		LOG.info("Number of calculation units: " + calcUnits.size());
		LOG.debug(calcUnits);

		// partition
		LOG.info("Partitioning calculation units (using a "+partitioner.getClass().getSimpleName()+")...");
		PartitionedCalculationUnitDAG partitionedUnits = partitioner.partition(calcUnits);
		LOG.info("Number of partitions: " + partitionedUnits.getNumPartitions());
		LOG.debug(partitionedUnits);

		// compile
		LOG.info("Compiling to plan...");
		MRPlan plan = compiler.compile(partitionedUnits, indir, outdir, scratchdir);
		LOG.info("Compilation finished.");
		LOG.debug(plan);
		
		return plan;

	}

}
