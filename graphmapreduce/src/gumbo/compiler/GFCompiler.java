/**
 * Created: 15 May 2014
 */
package gumbo.compiler;

import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.filemapper.FileMapper;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.PartitionedCalculationUnitGroup;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.compiler.resolver.CalculationCompiler;
import gumbo.compiler.resolver.DirManager;
import gumbo.compiler.structures.MRPlan;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class GFCompiler {
	

	private static final Log LOG = LogFactory.getLog(GFCompiler.class); 

	protected GFDecomposer decomposer;
	protected BGFE2CUConverter converter;
	protected CULinker linker;
	protected FileMapper filemapper;
	protected CalculationPartitioner partitioner;
	
	
	/**
	 * Default constructor, uses a {@link UnitPartitioner}.
	 */
	public GFCompiler() {
		this.partitioner = new UnitPartitioner();
	}

	public GFCompiler(CalculationPartitioner partitioner) {
		this.partitioner = partitioner;
	}

	/**
	 * @see GFCompiler#createPlan(Collection, RelationFileMapping, Path, Path)
	 * 
	 * @throws GFCompilerException
	 */
	public GumboPlan createPlan(GFExistentialExpression expression, RelationFileMapping infiles, Path outdir, Path scratchdir) throws GFCompilerException {
		HashSet<GFExistentialExpression> expressions = new HashSet<GFExistentialExpression>();
		expressions.add(expression);
		return createPlan(expressions, infiles, outdir, scratchdir);
	}

	/**
	 * Creates a {@link GumboPlan} for a given set of existential expressions.
	 * TODO #core convert rfm, outdir and scratchdir to set of special expressions
	 * @param expressions the set of expressions to convert
	 * @param indir the directory containing the input data
	 * @param outdir the directory where to put the output data
	 * @param scratchdir the directory the plan can use
	 * 
	 * @return A GumboPlan for calculation the GFE's
	 * 
	 * @throws GFCompilerException 
	 */
	public GumboPlan createPlan(Collection<GFExistentialExpression> expressions, RelationFileMapping infiles, Path outdir, Path scratchdir) throws GFCompilerException {

		// decomposer -> CUConverter -> CULinker -> file mappings -> partition
		
		// TODO #core implement
		
		// decompose expressions into basic ones
		LOG.info("Decomposing GFEs into basic GFEs (BGFEs)...");
		Set<GFExistentialExpression> bgfes = decomposer.decomposeAll(expressions);
		LOG.info("Number of BGFEs: " + bgfes.size());
		LOG.debug(bgfes);
		
		// CUConverter 
		LOG.info("Converting BGFEs into CalculationUnits (CUs)...");
		Map<RelationSchema, BasicGFCalculationUnit> cus = converter.createCalculationUnits(bgfes);
		LOG.info("Number of CUs: " + cus.size());
		LOG.debug(cus);
		
		// CULinker 
		LOG.info("Linking Calculation Units (CUs)...");
		CalculationUnitGroup dag = linker.createDAG(cus);
		LOG.info("Input relations: " + dag.size());
		LOG.info("Output relations: " + dag.size());
		LOG.info("Intermediate relations: " + dag.size());
		LOG.debug(dag);
		
		// file mappings 
		LOG.info("Creating initial file mapping...");
		DirManager dm = filemapper.expandFileMapping(infiles, dag);
		LOG.info("Input files: " + fm.size());
		LOG.info("Output files: " + fm.size());
		LOG.info("Intermediate files: " + fm.size());
		LOG.debug(fm);
		
		// partition
		LOG.info("Partitioning...");
		PartitonedCalculationUnitDAG pdag = partitioner.partition(dag,dm);
		LOG.info("Number of partitions: " + pdag.size());
		LOG.debug(pdag);
		
		return plan;

	}

}
