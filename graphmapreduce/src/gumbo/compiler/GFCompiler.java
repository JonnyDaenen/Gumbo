/**
 * Created: 15 May 2014
 */
package gumbo.compiler;

import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.FileMapper;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
		this(new UnitPartitioner());
	}

	public GFCompiler(CalculationPartitioner partitioner) {
		this.partitioner = partitioner;
		
		decomposer = new GFDecomposer();
		converter = new BGFE2CUConverter();
		linker = new CULinker();
		filemapper = new FileMapper();
	}

	/**
	 * Creates a {@link GumboPlan} for a given set of existential expressions.
	 * FUTURE convert rfm, outdir and scratchdir to set of special expressions?
	 * @param name 
	 * @param expressions the set of expressions to convert
	 * @param indir the directory containing the input data
	 * @param outdir the directory where to put the output data
	 * @param scratchdir the directory the plan can use
	 * 
	 * @return A GumboPlan for calculation the GFE's
	 * 
	 * @throws GFCompilerException 
	 */
	public GumboPlan createPlan(GumboQuery query) throws GFCompilerException {

		// decomposer -> CUConverter -> CULinker -> file mappings -> partition

		try {
			
			// adjust output directory
			// FUTURE make option to switch this off
			String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
			LOG.info("Adding suffix to scratch and output paths: "+ timeStamp);
			query.setOutput(query.getOutput().suffix(timeStamp));
			query.setScratch(query.getOutput().suffix(timeStamp));
			
			// decompose expressions into basic ones
			LOG.info("Decomposing GFEs into basic GFEs (BGFEs)...");
			Set<GFExistentialExpression> bgfes = decomposer.decomposeAll(query.getQueries());
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

			// intitial file mappings 
			LOG.info("Creating initial file mapping...");
			FileManager fm = filemapper.createFileMapping(query.getInputs(), query.getOutput(), query.getScratch(), dag);
			LOG.info("Input files: " + fm);
			LOG.info("Output files: " + fm);
			LOG.info("Intermediate files: " + fm);
			LOG.debug(fm);

			// partition
			LOG.info("Partitioning...");
			PartitionedCUGroup pdag = partitioner.partition(dag,fm);
			LOG.info("Number of partitions: " + pdag.getNumPartitions());
			LOG.debug(pdag);
			
			GumboPlan plan = new GumboPlan(query.getName(), pdag, fm);
			
			return plan;
			
		} catch (Exception e) {
			throw new GFCompilerException("Compiler error: " + e.getMessage(),e);
		}

		
		

	}

}
