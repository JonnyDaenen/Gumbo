/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.spark;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.ExecutionException;
import gumbo.engine.GFEngine;
import gumbo.engine.hadoop.PartitionQueue;
import gumbo.engine.spark.converter.GumboSparkConverter;
import gumbo.structures.data.RelationSchema;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

		// initialize
		initialize();

		// execute
		executeLevelwise(plan);

		// cleanup
		// TODO
	}

	/**
	 * 
	 */
	private void initialize() {
		// TODO this local stuff needs to be changed:
		SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Gumbo");
		// SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		ctx = new JavaSparkContext(sparkConf);
		
		// TODO close context in engine!

	}

	/**
	 * @param plan
	 * @throws ExecutionException
	 */
	private void executeLevelwise(GumboPlan plan) throws ExecutionException {

		try {
			GumboSparkConverter converter = new GumboSparkConverter(plan.getName(), plan.getFileManager(), null); // TODO add settings

			long start = System.nanoTime();

			// 1. create queue
			PartitionQueue queue = new PartitionQueue(plan.getPartitions());

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
					Map<RelationSchema, JavaRDD<String>> rdd = converter.convert(partition);

					// TODO update the queue
					//	queue.addRdds(partition, rdds);

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


}