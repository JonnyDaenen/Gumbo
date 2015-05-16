/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.hadoop.converter;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.FileMappingExtractor;
import gumbo.engine.hadoop.mrcomponents.input.GuardInputFormat;
import gumbo.engine.hadoop.mrcomponents.input.GuardTextInputFormat;
import gumbo.engine.hadoop.mrcomponents.round1.combiners.GFCombinerGuarded;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1GroupComparator;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1Partitioner;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1SortComparator;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardCsv;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardRel;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardRelOptimized;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardedCsv;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardedRel;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardedRelOptimized;
import gumbo.engine.hadoop.mrcomponents.round1.reducers.GFReducer1;
import gumbo.engine.hadoop.mrcomponents.round1.reducers.GFReducer1Optimized;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardCsv;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardRel;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardTextCsv;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardTextRel;
import gumbo.engine.hadoop.mrcomponents.round2.reducers.GFReducer2;
import gumbo.engine.hadoop.mrcomponents.round2.reducers.GFReducer2Text;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * A converter for transforming a {@link GumboPlan} into a Map-Reduce plan for hadoop.
 * Serialization is used to pass objects and settings to the mappers and reducers,
 * a file manager is used to create new locations for intermediate data.
 * 
 * 
 * @author Jonny Daenen
 * 
 */
public class GumboHadoopConverter {


	public class ConversionException extends Exception {

		private static final long serialVersionUID = 1L;


		public ConversionException(String msg) {
			super(msg);
		}


		public ConversionException(String msg, Exception e) {
			super(msg,e);
		}
	}

	private GFPrefixSerializer serializer;
	private HadoopExecutorSettings settings;
	private FileManager fileManager;
	private FileMappingExtractor extractor;

	private String queryName;


	private static final Log LOG = LogFactory.getLog(GumboHadoopConverter.class);

	/**
	 * 
	 */
	public GumboHadoopConverter(String queryName, FileManager fileManager, Configuration conf) {
		this.queryName = queryName;
		this.settings = new HadoopExecutorSettings(conf);
		this.fileManager = fileManager;

		this.extractor = new FileMappingExtractor();
		this.serializer = new GFPrefixSerializer();
	}

	/**
	 * Converts a {@link CalculationUnitGroup} into a map reduce job.
	 * {@link CalculationUnit}s of other types then {@link BasicGFCalculationUnit} are ignored.
	 * 
	 * @param cug 
	 * 			the group of calculations to convert
	 * @param fileManager 
	 * 			the file manager to be used during conversion
	 * 
	 * @return a Controlledjob with a map and a reduce phase
	 * 
	 * @throws ConversionException when wrong types of calculation units are present
	 */
	public List<ControlledJob> convert(CalculationUnitGroup cug) throws ConversionException {

		// extract expressions
		for (CalculationUnit cu : cug) {
			// check if all calculation units are basic ones
			if (! (cu instanceof BasicGFCalculationUnit)) {
				throw new ConversionException("Non-basic calculation unit found, type: " + cu.getClass());
			}
		}

		// create 2 jobs
		LinkedList<ControlledJob> jobs = new LinkedList<>();

		ControlledJob job1 = createRound1Job(cug);
		ControlledJob job2 = createRound2Job(cug,job1);

		jobs.add(job1);
		jobs.add(job2);

		// return jobs
		return jobs;

	}


	private ControlledJob createRound1Job(CalculationUnitGroup cug) throws ConversionException {

		try {

			// create job
			Job hadoopJob = Job.getInstance(settings.getConf()); // note: makes a copy of the conf

			/* GENERAL */
			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(getName(cug,1));

			// extract file mapping where input paths are made specific
			// TODO also filter out non-related relations
			RelationFileMapping mapping = extractor.extractFileMapping(fileManager);

			LOG.info(mapping);

			// wrapper object for expressions
			ExpressionSetOperations eso = new ExpressionSetOperations(extractExpressions(cug),mapping); 

			// pass arguments via configuration
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(eso.getExpressionSet()));
			conf.set("relationfilemapping", eso.getFileMapping().toString());


			/* MAPPER */


			// guarded mapper for relational files
			for ( Path guardedPath : eso.getGuardedRelPaths()) {
				LOG.info("Setting M1 guarded path to " + guardedPath + " using mapper " + GFMapper1GuardedRel.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardedPath, 
						TextInputFormat.class, GFMapper1GuardedRelOptimized.class);
			}

			// guarded mapper for csv files
			for ( Path guardedPath : eso.getGuardedCsvPaths()) {
				LOG.info("Setting M1 guarded path to " + guardedPath + " using mapper " + GFMapper1GuardedCsv.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardedPath, 
						TextInputFormat.class, GFMapper1GuardedCsv.class);
			}

			// WARNING: equal guarded paths are overwritten!
			// hence they are only processed once
			Collection<Path> commonPaths = eso.intersectGuardGuardedPaths();
			if (commonPaths.size() != 0) {
				LOG.warn("Guarded paths that are guard paths (and will only be processed by the corresponding guard mapper): " + commonPaths);
			}

			// guard mapper for relational files
			for ( Path guardPath : eso.getGuardRelPaths()) {
				LOG.info("Setting M1 guard path to " + guardPath + " using mapper " + GFMapper1GuardRel.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardPath, 
						TextInputFormat.class, GFMapper1GuardRelOptimized.class);
			}


			// guard mapper for csv files
			for ( Path guardPath : eso.getGuardCsvPaths()) {
				LOG.info("Setting M1 guard path to " + guardPath + " using mapper " + GFMapper1GuardCsv.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardPath, 
						TextInputFormat.class, GFMapper1GuardCsv.class);
			}

			// set the output path to a newly generated file
			Path intermediatePath = fileManager.getNewTmpPath(getName(cug,1));
			FileOutputFormat.setOutputPath(hadoopJob, intermediatePath);

			// set intermediate/mapper output
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(Text.class);

			/* COMBINER */
			if (settings.getBooleanProperty(AbstractExecutorSettings.guardedCombinerOptimizationOn)) {
				hadoopJob.setCombinerClass(GFCombinerGuarded.class);
			}

			/* REDUCER */

			// determine reducer
			hadoopJob.setReducerClass(GFReducer1Optimized.class); 
			Round1ReduceJobEstimator redestimator = new Round1ReduceJobEstimator(settings); // TODO fix estimate
			hadoopJob.setNumReduceTasks(redestimator.getNumReducers(eso.getExpressionSet(),mapping));

			// set reducer output
			// hadoopJob.setOutputKeyClass(NullWritable.class);
			hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(Text.class);

			// finite memory by sorting
			if (settings.getBooleanProperty(AbstractExecutorSettings.round1FiniteMemoryOptimizationOn)) {
				hadoopJob.setGroupingComparatorClass(Round1GroupComparator.class);
				hadoopJob.setSortComparatorClass(Round1SortComparator.class);
				hadoopJob.setPartitionerClass(Round1Partitioner.class);
			}


			return new ControlledJob(hadoopJob, null);

		} catch (IOException | GFOperationInitException e) {
			throw new ConversionException("Conversion failed. Cause: " + e.getMessage(),e);
		} 

	}


	/**
	 * Extracts the GF expressions from the group.
	 * @param cug
	 * @return
	 */
	private Collection<GFExistentialExpression> extractExpressions(CalculationUnitGroup cug) {

		Set<GFExistentialExpression> set = new HashSet<>();

		for(CalculationUnit cu : cug) {
			BasicGFCalculationUnit bcu = (BasicGFCalculationUnit) cu;
			set.add(bcu.getBasicExpression());
		}
		return set;
	}

	/**
	 * @param cug
	 * @return
	 */
	private String getName(CalculationUnitGroup cug, int round) {
		String name = "";
		for (RelationSchema rs : cug.getOutputRelations() ) {
			// TODO sort
			name += rs.getName()+"+";
		}
		return queryName + "_"+ name + "_"+round;
	}


	/**
	 * TODO #core make a clear list of the input/output types of this round, depending on the optimizations.
	 * @param cug
	 * @param previousJob
	 * @return
	 * @throws ConversionException
	 */
	private ControlledJob createRound2Job(CalculationUnitGroup cug, ControlledJob previousJob) throws ConversionException {


		try {

			Path inPath = FileOutputFormat.getOutputPath(previousJob.getJob());

			// create job
			Job hadoopJob = Job.getInstance(settings.getConf()); // note: creates a copy of the conf

			/* GENERAL */
			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(getName(cug,2));


			// extract file mapping where input paths are made specific
			// TODO also filter out non-related relations
			RelationFileMapping mapping = extractor.extractFileMapping(fileManager);
			// wrapper object for expressions
			ExpressionSetOperations eso = new ExpressionSetOperations(extractExpressions(cug),mapping); 


			// pass arguments via configuration
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(eso.getExpressionSet()));
			conf.set("relationfilemapping", eso.getFileMapping().toString());



			/* MAPPER */

			// some optimizations require a special mapper round
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveReductionOn) ||
					settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {

				// add special non-identity mapper to process the guard input again

				// direct them to the special mapper (rel)
				for (Path guardPath : eso.getGuardRelPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + GFMapper2GuardRel.class.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, getRound2GuardMapperClass("rel"));
				}

				// direct them to the special mapper (csv)
				for (Path guardPath : eso.getGuardCsvPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + GFMapper2GuardCsv.class.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, getRound2GuardMapperClass("csv"));
				}

				// other files just need to be read and pushed to the reducer
				LOG.info("Adding M2 normal path " + inPath + " using identity mapper ");
				MultipleInputs.addInputPath(hadoopJob, inPath, 
						getRound2MapInputFormat(), Mapper.class);

			} else {

				// if keep-alive messages are allowed, we use the identity mapper

				LOG.info("Adding M2 path " + inPath + " using identity mapper ");
				hadoopJob.setMapperClass(Mapper.class);
				FileInputFormat.addInputPath(hadoopJob, inPath);


				// FIXME #core when keep-alives are on and tuplepointeropt is also on, guard tuples are not generated.
				// we use a custom input class to allow the mapper to output key-value pairs again
				hadoopJob.setInputFormatClass(getRound2MapInputFormat());
			}

			// set intermediate/mapper output
			// if we use a tuplepointeroptimization
			// we cannot use the ints for now, as we need to re-send the tuple itself
			hadoopJob.setMapOutputKeyClass(Text.class);
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {
				hadoopJob.setMapOutputValueClass(Text.class); // OPTIMIZE make it a combined class
			} else {
				hadoopJob.setMapOutputValueClass(IntWritable.class); // FUTURE make it a list? for combiner
			}


			/* REDUCER */

			// the reducer reads text or int format, depending on the optimization
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {
				hadoopJob.setReducerClass(GFReducer2Text.class);
			} else {
				hadoopJob.setReducerClass(GFReducer2.class);
			}

			Round2ReduceJobEstimator redestimator = new Round2ReduceJobEstimator(conf);
			hadoopJob.setNumReduceTasks(redestimator.getNumReducers(eso.getExpressionSet(),mapping));

			// create dummy output path, as individual relations are sent to specific locations
			Path dummyPath = fileManager.getNewTmpPath(getName(cug,2));
			FileOutputFormat.setOutputPath(hadoopJob, dummyPath);



			// set reducer output, we do not output keys, only values
			hadoopJob.setOutputKeyClass(NullWritable.class);
			//hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(Text.class);


			/* CREATE JOB */
			ControlledJob newjob = new ControlledJob(hadoopJob, null);
			newjob.addDependingJob(previousJob);

			return newjob;

		} catch (IOException | GFOperationInitException e) {
			throw new ConversionException("Conversion failed. Cause: " + e.getMessage(),e);
		} 


	}

	/**
	 * Determines the mapper class of the second round, based on optimiations.
	 * Optimizations taken into account are Tuple Pointer, the file type is also taken
	 * into account.
	 * 
	 * @param string the file type (csv/rel)
	 * 
	 * @return a suiting mapper
	 */
	@SuppressWarnings("rawtypes")
	private Class<? extends Mapper> getRound2GuardMapperClass(String string) {

		if (string.equals("csv") ) {
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {
				return GFMapper2GuardTextCsv.class;
			} else {
				return GFMapper2GuardCsv.class;
			}
		} else {
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {
				return GFMapper2GuardTextRel.class;
			} else {
				return GFMapper2GuardRel.class;
			}
		}
	}

	/**
	 * Returns a suitable input format for the second round mapper.
	 * The class is determined by the Tuple Pointer optimization:
	 * when this optimization is inactive, only int's are transmitted as the values,
	 * which can be easily processed. Otherwise, when the guard relation needs to
	 * be reread, we also need to parse text and hence we set the input class to text.
	 * 
	 * @return a suitable input format
	 */
	@SuppressWarnings("rawtypes")
	private Class<? extends InputFormat> getRound2MapInputFormat() {
		Class<? extends InputFormat> atomInputFormat = GuardInputFormat.class;
		if (settings.getBooleanProperty(HadoopExecutorSettings.guardAddressOptimizationOn)) {
			atomInputFormat = GuardTextInputFormat.class;
		}
		return atomInputFormat;
	}

}
