/**
 * Created: 22 Aug 2014
 */
package gumbo.executor.spark;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.structures.MRPlan;
import gumbo.compiler.structures.operations.MRJob;
import gumbo.executor.ExecutionException;
import gumbo.executor.GFExecutor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Executes an MR-plan on Spark.
 * 
 * @author Jonny Daenen
 * 
 */
public class SparkExecutor implements GFExecutor {

	private static final Log LOG = LogFactory.getLog(SparkExecutor.class);

	JavaSparkContext ctx;

	public void execute(MRPlan plan) throws ExecutionException {

		// TODO are the jobs merged already?

		// initialize
		initialize();

		// execute
		executeLevelwise(plan);

		// cleanup

	}

	/**
	 * 
	 */
	private void initialize() {
		// TODO this local stuff needs to be changed:
		SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Fronjo");
		// SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		ctx = new JavaSparkContext(sparkConf);

	}

	/**
	 * @param plan
	 * @throws ExecutionException
	 */
	private void executeLevelwise(MRPlan plan) throws ExecutionException {

		Map<MRJob, JavaRDD<String>> rdds = new HashMap<>();

		// convert jobs to rdds
		LOG.debug("Converting jobs ");
		for (MRJob job : plan.getJobsLevelwise()) {
			LOG.debug("Converting job: " + job);
			JavaRDD<String> rdd = convertToRDD(job, rdds, plan.getInputPaths());
			rdds.put(job, rdd);
		}

		// get output
		for (MRJob job : plan.getOutputJobs()) {
			if (!rdds.containsKey(job))
				throw new ExecutionException("Job not converted yet: " + job);
			JavaRDD<String> rdd = rdds.get(job);
			Path outfile = plan.getOutputFolder().suffix(Path.SEPARATOR + job.getOutputPath().getName());

			LOG.debug("Writing to output file: " + outfile);
			rdd.saveAsTextFile(outfile.toString());
		}

	}

	/**
	 * Constructs an RDD without applying an action to it.
	 * 
	 * @param job
	 *            the job to convert to an RDD
	 * @param rdds
	 *            a map linking each job to its RDD
	 * @param inPaths
	 *            the file/folder where to get raw input
	 * @return an RDD for the job
	 * @throws ExecutionException
	 */
	private JavaRDD<String> convertToRDD(MRJob job, Map<MRJob, JavaRDD<String>> rdds, RelationFileMapping inPaths)
			throws ExecutionException {

		// calculate input dataset
		JavaRDD<String> input = ctx.parallelize(new LinkedList<String>());

		// input file
		for (Path p : job.getInputPaths()) {
			if (inPaths.containsPath(p)) {
				LOG.debug("Using input path: " + inPaths);
				input = ctx.textFile(p.toString());
			}
		}
		
		// dependency RDDs
		for (MRJob jobdep : job.getDependencies()) {
			if (!rdds.containsKey(jobdep))
				throw new ExecutionException("A job dependency has not been converted yet: " + jobdep);

			input = input.union(rdds.get(jobdep));
		}

		// perform operations
		try {
			GFMapperSpark mapper = new GFMapperSpark(job.getMapClass(), job.getGFExpressions());
			GFReducerSpark reducer = new GFReducerSpark(job.getReduceClass(), job.getGFExpressions());
			JavaPairRDD<String, String> mapped = input.flatMapToPair(mapper);
//			List<Tuple2<String, String>> result = mapped.collect();
//			LOG.debug("Map result:" + result.size());
//			for (Tuple2<String, String> tuple2 : result) {
//				LOG.debug(tuple2._1 + " --- " + tuple2._2);
//			}
			
			JavaPairRDD<String, Iterable<String>> grouped = mapped.groupByKey();
			JavaRDD<String> reduced = grouped.flatMap(reducer);
			
			List<String> resultred = reduced.collect();
			LOG.debug("Reduce result:" + resultred.size());
			for (String s : resultred) {
				LOG.debug(s);
			}
			

			return reduced;
		} catch (InstantiationException e) {
			throw new ExecutionException("Cannot instantiate mapper/reducer: " + e.getMessage(), e);
		} catch (IllegalAccessException e) {
			throw new ExecutionException("Cannot instantiate mapper/reducer: " + e.getMessage(), e);
		}

	}

}
