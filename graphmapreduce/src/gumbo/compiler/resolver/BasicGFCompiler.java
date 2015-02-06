/**
 * Created: 09 May 2014
 */
package gumbo.compiler.resolver;

import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.resolver.mappers.GFMapper1AtomBased;
import gumbo.compiler.resolver.mappers.GFMapper2Generic;
import gumbo.compiler.resolver.reducers.GFReducer1AtomBased;
import gumbo.compiler.resolver.reducers.GFReducer2Generic;
import gumbo.compiler.resolver.reducers.GuardedProjectionReducer;
import gumbo.compiler.structures.MRJob;
import gumbo.compiler.structures.MRJob.MRJobType;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * Compiles a set of independent basic GF expressions into a 2-round MR-job.
 * 
 * 
 * @author Jonny Daenen
 * 
 */
public class BasicGFCompiler {

	DirManager dirManager;
	private GFPrefixSerializer serializer;

	public BasicGFCompiler(DirManager dirCreator, GFPrefixSerializer serializer, String jobnamePrefix) {
		this.dirManager = dirCreator;
		this.serializer = serializer;
	}

	/**
	 * Compiles a set of basic GF expressions into a 2-round MR-job. Each
	 * calculation unit's output is sent to its own subdir.
	 * 
	 * @param partition
	 * @return
	 * @throws UnsupportedCalculationUnitException
	 * @throws CompilerException
	 * 
	 * @pre CalculationUnits are independent TODO maybe check independence?
	 */
	public Map<CalculationUnit, Set<MRJob>> compileBasicGFCalculationUnit(CalculationUnitGroup partition)
			throws UnsupportedCalculationUnitException, CompilerException {

		// determine path suffix for intermediate dirs
		String suffix = "";
		for (CalculationUnit cu : partition) {
			suffix += "_" + cu.getOutputSchema().getName() + cu.getOutputSchema().getNumFields();
		}
		suffix = suffix.substring(1); // FIXME this could crash when there are
										// no partitions!

		// load paths
		// TODO make sure this is really bottom up, as we assume the previous levels are already coupled to temp files
		Set<Path> inputs = dirManager.lookup(partition.getInputRelations());

		Path tmpDir = dirManager.getNewTmpPath(suffix);
		Set<Path> tmpDirs = new HashSet<Path>();
		tmpDirs.add(tmpDir);

		// FUTURE multiple outputs
		// Set<Path> outputs =
		// dirManager.lookup(partition.getOutputRelations());
		// if(outputs.size() != 1) {
		// throw new
		// CompilerException("Only 1 output path per partition is supported, found "
		// + outputs.size());
		// }
		//
		// Path output = outputs.iterator().next();

		// for now, we use 1 path for this entire partition

		Path output = dirManager.getNewOutPath(suffix);

		// assemble expressions
		Set<GFExistentialExpression> expressions = new HashSet<GFExistentialExpression>();

		for (CalculationUnit cu : partition) {
			if (!(cu instanceof BasicGFCalculationUnit)) {
				throw new UnsupportedCalculationUnitException("Unsupported Calculation Class: "
						+ cu.getClass().getSimpleName());
			}

			BasicGFCalculationUnit bcu = (BasicGFCalculationUnit) cu;
			if (bcu.getBasicExpression().isNonConjunctiveBasicGF()) {
				// OPTIMIZE use 1-round MR
			}
			expressions.add(bcu.getBasicExpression());

			// update output path for this schema
			RelationSchema schema = bcu.getOutputSchema();
			// CLEAN
			// other
			// object
			// to
			// do
			// this
			dirManager.updatePath(schema,
					output.suffix(Path.SEPARATOR + GuardedProjectionReducer.generateFolder(schema))); 

		}

		// create 2 rounds
		Set<MRJob> jobs = new HashSet<MRJob>();

		String name = generateName(partition);

		try {

			MRJob round1job = createBasicGFRound1Job(inputs, tmpDir, expressions, name + "_R1");
			MRJob round2job = createBasicGFRound2Job(tmpDirs, output, expressions, name + "_R2");
			round2job.addDependingJob(round1job);

			jobs.add(round1job);
			jobs.add(round2job);
		} catch (IOException e) {
			throw new CompilerException("Error during creation of MR round: " + e.getMessage());
		}

		// each expression is mapped onto the same job
		Map<CalculationUnit, Set<MRJob>> result = new HashMap<CalculationUnit, Set<MRJob>>();
		for (CalculationUnit cu : partition) {
			result.put(cu, jobs);

		}

		return result;

	}

	/**
	 * @param partition
	 * @return
	 */
	private String generateName(CalculationUnitGroup partition) {
		StringBuilder builder = new StringBuilder();
		for (CalculationUnit calculationUnit : partition) {
			builder.append(calculationUnit.getOutputSchema().getName() + "_");
		}

		return "Fronjo_calc_" + builder.toString();
	}

	/**
	 * Creates a first round job.
	 */
	private MRJob createBasicGFRound1Job(Set<Path> in, Path out, Set<GFExistentialExpression> set, String name)
			throws IOException {

		MRJob job = new MRJob(name);
		job.setType(MRJobType.GF_ROUND1);

		job.addInputPaths(in);
		job.setOutputPath(out);

//		job.setMapFunction(GFMapper1Generic.class);
//		job.setReduceFunction(GFReducer1Generic.class);
		
		job.setMapFunction(GFMapper1AtomBased.class);
		job.setReduceFunction(GFReducer1AtomBased.class);
		
		job.setExpressions(set);
		
		job.setOutputJob(false);

		return job;
	}

	/**
	 * Creates a second round job.
	 */
	private MRJob createBasicGFRound2Job(Set<Path> in, Path out, Set<GFExistentialExpression> set, String name)
			throws IOException {

		MRJob job = new MRJob(name);

		job.setType(MRJobType.GF_ROUND2);
		
		job.addInputPaths(in);
		job.setOutputPath(out);

		job.setMapFunction(GFMapper2Generic.class);
		job.setReduceFunction(GFReducer2Generic.class);
		job.setExpressions(set);
	
		job.setOutputJob(true);

		return job;
	}

}
