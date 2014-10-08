/**
 * Created: 09 May 2014
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapreduce.guardedfragment.planner.calculations.BasicGFCalculationUnit;
import mapreduce.guardedfragment.planner.calculations.CalculationUnit;
import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;
import mapreduce.guardedfragment.planner.compiler.CompilerException;
import mapreduce.guardedfragment.planner.compiler.DirManager;
import mapreduce.guardedfragment.planner.compiler.UnsupportedCalculationUnitException;
import mapreduce.guardedfragment.planner.compiler.mappers.GuardedBooleanMapper;
import mapreduce.guardedfragment.planner.compiler.mappers.GuardedMapper;
import mapreduce.guardedfragment.planner.compiler.reducers.GuardedAppearanceReducer;
import mapreduce.guardedfragment.planner.compiler.reducers.GuardedProjectionReducer;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Compiles a set of independent basic GF expressions into a 2-round MR-job.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class OldBasicGFCompiler {

	DirManager dirManager;
	private GFPrefixSerializer serializer;
	

	public OldBasicGFCompiler(DirManager dirCreator, GFPrefixSerializer serializer, String jobnamePrefix) {
		this.dirManager = dirCreator;
		this.serializer = serializer;
	}
	
	/**
	 * Compiles a set of basic GF expressions into a 2-round MR-job.
	 * Each calculation unit's output is sent to its own subdir.
	 * 
	 * @param partition
	 * @return
	 * @throws UnsupportedCalculationUnitException 
	 * @throws CompilerException 
	 * 
	 * @pre CalculationUnits are independent
	 * TODO maybe check independence?
	 */
	public Map<CalculationUnit, Set<ControlledJob>> compileBasicGFCalculationUnit(CalculationUnitDAG partition) throws UnsupportedCalculationUnitException, CompilerException {
		
		// determine path suffix for intermediate dirs
		String suffix = "";
		for (CalculationUnit cu : partition) {
			suffix +=  "_" + cu.getOutputSchema().getName()+ cu.getOutputSchema().getNumFields();
		}
		suffix = suffix.substring(1);
		
		// load paths
		Set<Path> inputs = dirManager.lookup(partition.getInputRelations());
		
		Path tmpDir = dirManager.getNewTmpPath(suffix);
		Set<Path> tmpDirs = new HashSet<Path>();
		tmpDirs.add(tmpDir);
		
		// FUTURE multiple outputs
//		Set<Path> outputs = dirManager.lookup(partition.getOutputRelations());	
//		if(outputs.size() != 1) {
//			throw new CompilerException("Only 1 output path per partition is supported, found " + outputs.size());
//		}
//		
//		Path output = outputs.iterator().next();
		
		// for now, we use 1 path for this entire partition
		
		Path output = dirManager.getNewOutPath(suffix);
		
		
		
		// assemble expressions
		Set<GFExistentialExpression> expressions = new HashSet<GFExistentialExpression>();
		
		for (CalculationUnit cu : partition) {
			if(! (cu instanceof BasicGFCalculationUnit)){
				throw new UnsupportedCalculationUnitException("Unsupported Calculation Class: " + cu.getClass().getSimpleName());
			}
			
			BasicGFCalculationUnit bcu = (BasicGFCalculationUnit) cu;	
			if(bcu.getBasicExpression().isNonConjunctiveBasicGF()){
				// OPTIMIZE use 1-round MR
			}
			expressions.add(bcu.getBasicExpression());
			
			// update output path
			RelationSchema schema = bcu.getOutputSchema();
			dirManager.updatePath(schema, output.suffix(Path.SEPARATOR+GuardedProjectionReducer.generateFolder(schema))); // CLEAN other object to do this
			
		}
		
		// create 2 rounds 
		Set<ControlledJob> jobs = new HashSet<ControlledJob>();
		
		String name = generateName(partition);
		
		try {
			
			ControlledJob round1job = createBasicGFRound1Job(inputs, tmpDir, expressions, name+"_R1");
			ControlledJob round2job = createBasicGFRound2Job(tmpDirs, output, expressions, name+"_R2");
			round2job.addDependingJob(round1job);
			
			jobs.add(round1job);
			jobs.add(round2job);
		} catch (IOException e) {
			throw new CompilerException("Error during creationg of MR round: " + e.getMessage() );
		}
		
		
		// each expression is mapped onto the same job 
		Map<CalculationUnit, Set<ControlledJob>> result = new HashMap<CalculationUnit, Set<ControlledJob>>();
		for (CalculationUnit cu : partition) {
			result.put(cu, jobs);
			
		}
		
		return result ;
		
	}

	/**
	 * @param partition
	 * @return
	 */
	private String generateName(CalculationUnitDAG partition) {
		StringBuilder builder = new StringBuilder();
		for (CalculationUnit calculationUnit : partition) {
			builder.append(calculationUnit.getOutputSchema().getName()+"_");
		}
		
		
		return "Fronjo_calc_"+ builder.toString();
	}

	private ControlledJob createBasicGFRound1Job(Set<Path> in, Path out, Set<GFExistentialExpression> set, String name) throws IOException {
		// create basic job
		Job job = createJob(in, out, name);

		// set mapper an reducer
		job.setMapperClass(GuardedMapper.class);
		job.setReducerClass(GuardedAppearanceReducer.class);
		
		// set guard and guarded set
		Configuration conf = job.getConfiguration();
		conf.set("formulaset", serializer.serializeSet(set));

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return new ControlledJob(job, null);
	}

	private ControlledJob createBasicGFRound2Job(Set<Path> in, Path out, Set<GFExistentialExpression> set, String name) throws IOException {

		// create basic job
		Job job = createJob(in, out, name);

		// set mapper an reducer
		job.setMapperClass(GuardedBooleanMapper.class);
		job.setReducerClass(GuardedProjectionReducer.class);
		
		
		Configuration conf = job.getConfiguration();
		conf.set("formulaset", serializer.serializeSet(set));

		// set intermediate/mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set reducer output
		// CLEAN maybe this can be removed?
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputValueClass(KeyBasedMultipleTextOutputFormat.class);
		
		// avoid empty files
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); 

		return new ControlledJob(job, null);
	}


	

	/**
	 * Creates a basic job with IO folders set correctly.
	 * 
	 * @param in
	 * @param out
	 * @param name 
	 * @throws IOException
	 * @returna job configured with IO folders
	 */
	private Job createJob(Set<Path> in, Path out, String name) throws IOException {
		// create job
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(name);

		// set IO
		for (Path inpath : in) {
			FileInputFormat.addInputPath(job, inpath);
		}
		FileOutputFormat.setOutputPath(job, out);

		return job;
	}

	
}