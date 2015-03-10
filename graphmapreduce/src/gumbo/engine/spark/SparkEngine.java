/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.spark;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.ExecutionException;
import gumbo.engine.GFEngine;
import gumbo.engine.settings.ExecutorSettings;
import gumbo.engine.spark.converter.GumboSparkConverter;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Executes an MR-plan on Spark.
 * 
 * @author Jonny Daenen
 * 
 */
public class SparkEngine implements GFEngine {

	private static final Log LOG = LogFactory.getLog(SparkEngine.class);
	private static final long REFRESH_WAIT = 500; // FUTURE increase to 5000ms
	JavaSparkContext ctx;

	@Override
	public void execute(GumboPlan plan) throws ExecutionException {

		executePlan(plan,null);  // TODO hadoop config??
	}

	/**
	 * 
	 */
	private void initialize() {
		// TODO this local stuff needs to be changed:
		SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Gumbo");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//		sparkConf.set("spark.kryo.registrator", "org.kitesdk.examples.spark.AvroKyroRegistrator");
//		sparkConf.registerKryoClasses(
//				gumbo.structures.gfexpressions.operations.ExpressionSetOperations.class
//				);
		// SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		ctx = new JavaSparkContext(sparkConf);

		// TODO close context in engine!

	}

	/**
	 * @param plan
	 * @param conf 
	 * @throws ExecutionException
	 */
	private void executeLevelwise(GumboPlan plan, Configuration conf) throws ExecutionException {

		try {
			GumboSparkConverter converter = new GumboSparkConverter(plan.getName(), plan.getFileManager(), new ExecutorSettings(), ctx, conf); // TODO load real settings

			long start = System.nanoTime();

			// 1. create queue
			SparkPartitionQueue queue = new SparkPartitionQueue(plan.getPartitions());

			// 2. process jobs in queue
			LOG.info("Processing partition queue.");
			// while not all completed 
			while (!queue.isEmpty()) {
				// update states
				Set<CalculationUnitGroup> newPartitions = queue.updateStatus();


				//				LOG.info("Partitions to add: " + newPartitions.size());

				// for each job for which the dependencies are done
				// (and whose input files are now available for analysis)
				for (CalculationUnitGroup partition: newPartitions) {

					// convert job to hadoop job using only files 
					// (glob and directory paths are converted)
					converter.convert(partition);

					// update the queue
					queue.setFinished(partition);

				}

				// sleep
				Thread.sleep(REFRESH_WAIT);
			}

			long stop = System.nanoTime();

			// 3. remove intermediate data
			// TODO do this in loop?

			// 4. report outcome

			System.out.println("Running time: " + (stop-start)/1000000 + "ms");
			//		printCounters(jc);
			//		printJobDAG(jc);

		} catch (Exception e) {
			LOG.error("Spark engine was interrupted: " + e.getMessage());
			throw new ExecutionException("Spark engine interrupted.", e);
		}


	}


	public void executePlan(GumboPlan plan, Configuration conf) throws ExecutionException {
		
		
		// initialize
		initialize();

		// execute
		executeLevelwise(plan,conf);

		// cleanup
		// TODO

	}


}
