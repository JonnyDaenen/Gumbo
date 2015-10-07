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
import gumbo.engine.general.grouper.Decomposer;
import gumbo.engine.general.grouper.Grouper;
import gumbo.engine.general.grouper.GrouperFactory;
import gumbo.engine.general.grouper.GroupingPolicies;
import gumbo.engine.general.grouper.costmodel.CostCalculator;
import gumbo.engine.general.grouper.costmodel.CostSheet;
import gumbo.engine.general.grouper.costmodel.GGTCostCalculator;
import gumbo.engine.general.grouper.costmodel.MRSettings;
import gumbo.engine.general.grouper.policies.CostBasedGrouper;
import gumbo.engine.general.grouper.policies.GroupingPolicy;
import gumbo.engine.general.grouper.policies.KeyGrouper;
import gumbo.engine.general.grouper.policies.NoneGrouper;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.general.utils.FileMappingExtractor;
import gumbo.engine.hadoop.mrcomponents.input.GuardTextInputFormat;
import gumbo.engine.hadoop.mrcomponents.round1.combiners.GFCombinerGuarded;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1GroupComparator;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1Partitioner;
import gumbo.engine.hadoop.mrcomponents.round1.comparators.Round1SortComparator;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardCsv;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardRelOptimized;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardedCsv;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1GuardedRelOptimized;
import gumbo.engine.hadoop.mrcomponents.round1.reducers.GFReducer1Optimized;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardCsv;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GFMapper2GuardRelOptimized;
import gumbo.engine.hadoop.mrcomponents.round2.reducers.GFReducer2Optimized;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * FIXME this is a becoming a code mess and needs a thorough cleanup!
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
	 * @param grouper 
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

		Set<ControlledJob> jobs1 = createRound1Jobs(cug);
		ControlledJob job2 = createRound2Job(cug,jobs1);

		jobs.addAll(jobs1);
		jobs.add(job2);

		// return jobs
		return jobs;

	}

	private Set<ControlledJob> createRound1Jobs(CalculationUnitGroup cug) throws ConversionException {

		Set<ControlledJob> jobs = new HashSet<>();

		// create one master group to contain everything
		Collection<GFExistentialExpression> expressions = extractExpressions(cug);

		// extract file mapping where input paths are made specific
		// TODO also filter out non-related relations
		extractor.setIncludeOutputDirs(false); // TODO the grouper should filter out the non-existing files
		RelationFileMapping mapping1 = extractor.extractFileMapping(fileManager);
		
		// get the correct grouper
		Grouper grouper = GrouperFactory.createGrouper(mapping1, settings);

		// apply grouping
		extractor.setIncludeOutputDirs(true);
		RelationFileMapping mapping = extractor.extractFileMapping(fileManager);
		List<CalculationGroup> groups = grouper.group(cug);
		Decomposer decomposer = new Decomposer();

		MRSettings mrSettings = new MRSettings(settings);

		// create a job for each group
		for (CalculationGroup group : groups) {
			// get number of reducers 
			long mbytes = (group.getGuardedOutBytes() + group.getGuardedOutBytes()) / (1024*1024);
			int numReducers = (int) Math.ceil(( mbytes / mrSettings.getRedChunkSizeMB()));
			
			// create the MR job
			ControlledJob groupJob = createRound1Job(group, mapping, numReducers);
			jobs.add(groupJob);
		}

		return jobs;
	}


	private ControlledJob createRound1Job(CalculationGroup cug, RelationFileMapping mapping, int numRed) throws ConversionException {

		try {

			// create job
			Job hadoopJob = Job.getInstance(settings.getConf()); // note: makes a copy of the conf

			/* GENERAL */
			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(getName(cug,1));



			LOG.info(mapping);

			// wrapper object for expressions
			ExpressionSetOperations eso = new ExpressionSetOperations(cug.getExpressions(),cug.getRelevantExpressions(),mapping); 

			// pass arguments via configuration
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(eso.getExpressionSet()));
			conf.set("relationfilemapping", eso.getFileMapping().toString());
			conf.set("allexpressions", serializer.serializeSet(cug.getRelevantExpressions()));


			/* MAPPER */


			// guarded mapper for relational files
			for ( Path guardedPath : eso.getGuardedRelPaths()) {
				LOG.info("Setting M1 guarded path to " + guardedPath + " using mapper " + GFMapper1GuardedRelOptimized.class.getName());
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
				LOG.info("Setting M1 guard path to " + guardPath + " using mapper " + GFMapper1GuardRelOptimized.class.getName());
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
			//			Round1ReduceJobEstimator redestimator = new Round1ReduceJobEstimator(settings); // TODO fix estimate
			//			hadoopJob.setNumReduceTasks(redestimator.getNumReducers(eso.getExpressionSet(),mapping));

			hadoopJob.setNumReduceTasks(numRed);
			LOG.info("Setting Reduce tasks to " + numRed);

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
	 * Constructs a name for a group of calculations.
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
	 * Constructs a name for a group of calculations.
	 */
	private String getName(CalculationGroup cug, int round) {
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
	 * @param jobs1
	 * @return
	 * @throws ConversionException
	 */
	private ControlledJob createRound2Job(CalculationUnitGroup cug, Set<ControlledJob> jobs1) throws ConversionException {


		try {

			Set<Path> inPaths = new HashSet<Path>();
			for (ControlledJob job : jobs1) {

				Path inPath = FileOutputFormat.getOutputPath(job.getJob());
				inPaths.add(inPath);
			}


			// create job
			Job hadoopJob = Job.getInstance(settings.getConf()); // note: creates a copy of the conf

			/* GENERAL */
			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(getName(cug,2));


			// extract file mapping where input paths are made specific
			// TODO also filter out non-related relations
			RelationFileMapping mapping = extractor.extractFileMapping(fileManager);
			
			// wrapper object for expressions
			// expressions have _all_ the expressions in them
			Collection<GFExistentialExpression> allExpressions = extractExpressions(cug);
			ExpressionSetOperations eso = new ExpressionSetOperations(allExpressions,allExpressions,mapping); 


			// pass arguments via configuration
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(eso.getExpressionSet()));
			conf.set("relationfilemapping", eso.getFileMapping().toString());
			conf.set("allexpressions", serializer.serializeSet(allExpressions));



			/* MAPPER */

			// some optimizations require a special mapper round
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveOptimizationOn) ||
					settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {

				// add special non-identity mapper to process the guard input again

				// direct them to the special mapper (rel)
				Class<? extends Mapper> relClass = getRound2GuardMapperClass("rel");
				for (Path guardPath : eso.getGuardRelPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + relClass.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, relClass);
				}

				// direct them to the special mapper (csv)
				Class<? extends Mapper> csvClass = getRound2GuardMapperClass("csv");
				for (Path guardPath : eso.getGuardCsvPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + csvClass.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, csvClass );
				}

				// other files just need to be read and pushed to the reducer

				for (Path inPath : inPaths) {
					LOG.info("Adding M2 normal path " + inPath + " using identity mapper ");
					MultipleInputs.addInputPath(hadoopJob, inPath, 
							getRound2MapInputFormat(), Mapper.class);
				}
			} else {

				// if keep-alive messages are allowed, we use the identity mapper

				for (Path inPath : inPaths) {
					LOG.info("Adding M2 path " + inPath + " using identity mapper ");
					hadoopJob.setMapperClass(Mapper.class);
					FileInputFormat.addInputPath(hadoopJob, inPath);
				}

				// FIXME #core when keep-alives are on and tuplepointeropt is also on, guard tuples are not generated.
				// we use a custom input class to allow the mapper to output key-value pairs again
				hadoopJob.setInputFormatClass(getRound2MapInputFormat());
			}

			// set intermediate/mapper output
			// if we use a tuplepointeroptimization
			// we cannot use the ints for now, as we need to re-send the tuple itself
			hadoopJob.setMapOutputKeyClass(Text.class);
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
				hadoopJob.setMapOutputValueClass(Text.class); // OPTIMIZE make it a combined class
			} else {
				hadoopJob.setMapOutputValueClass(Text.class); // FUTURE make it a list? for combiner
			}


			/* REDUCER */

			// the reducer reads text or int format, depending on the optimization
			//			if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
			//				hadoopJob.setReducerClass(GFReducer2Text.class);
			//			} else {
			//				hadoopJob.setReducerClass(GFReducer2.class);
			//			}

			hadoopJob.setReducerClass(GFReducer2Optimized.class);

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

			for (ControlledJob job : jobs1) {
				newjob.addDependingJob(job);
			}
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
			return GFMapper2GuardCsv.class;
		} else {
			return GFMapper2GuardRelOptimized.class;
		}


		// --- old
		//		if (string.equals("csv") ) {
		//			if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
		//				return GFMapper2GuardTextCsv.class;
		//			} else {
		//				return GFMapper2GuardCsv.class;
		//			}
		//		} else {
		//			if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
		//				return GFMapper2GuardTextRel.class;
		//			} else {
		//				return GFMapper2GuardRel.class;
		//			}
		//		}
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
		return GuardTextInputFormat.class;
		//		Class<? extends InputFormat> atomInputFormat = GuardInputFormat.class;
		//		if (settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn)) {
		//			atomInputFormat = GuardTextInputFormat.class;
		//		}
		//		return atomInputFormat;
	}



}
