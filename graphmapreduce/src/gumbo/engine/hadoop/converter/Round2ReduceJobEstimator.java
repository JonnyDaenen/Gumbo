/**
 * Created: 14 Jan 2015
 */
package gumbo.engine.hadoop.converter;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.utils.estimation.RandomTupleEstimator;
import gumbo.utils.estimation.TupleEstimator;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Estimates the number of reducers needed for round 2.
 * This naïve estimator takes 3x the guard size.
 * 
 * features:
 * - newline correction
 * - csv correction
 * 
 * @author Jonny Daenen
 *
 */
public class Round2ReduceJobEstimator {


	private Configuration conf;


	/**
	 * 
	 */
	public Round2ReduceJobEstimator( Configuration conf, TupleEstimator tupleEstimator) {
		this.conf = conf;
	}

	/**
	 * 
	 */
	public Round2ReduceJobEstimator(Configuration conf) {
		this(conf, new RandomTupleEstimator(1024,10));
	}



	private static final Log LOG = LogFactory.getLog(Round2ReduceJobEstimator.class);


	/**
	 * @param job
	 * @param dirManager
	 * @return
	 */
	public int getNumReducers(Collection<GFExistentialExpression> exps, RelationFileMapping mapping) {
		int num;
		long bytesize = 0;


		for (GFExistentialExpression e: exps) {

			bytesize += estimateSize(e, mapping);
		}
		// TODO #multiquery compensate for multi-query
		// this can be done by taking the set of guards 
		// (and hence removing duplicates)
		// and calculating the total size.


		LOG.info("Output estimate " + bytesize);
		// 1 GB per reducer
		num = (int) Math.ceil(bytesize/1000000000.0);
		LOG.info("Reducer estimate " + num);

		return Math.max(1,num);
	}


	/**
	 * Calculates the total size of the guard.
	 * 
	 * @param e a guarded expression
	 * @param rfm relation-file mapping
	 * @return the size of a 
	 */
	private long estimateSize(GFExistentialExpression e, RelationFileMapping rfm) {
		long totalBytes = 0;
		try {
		FileSystem fs = FileSystem.get(conf);
		for (Path p : rfm.getPaths(e.getGuard().getRelationSchema())) {
			FileStatus fstatus = fs.getFileStatus(p);
			totalBytes += fstatus.getBlockSize(); 
			
		}
		} catch (Exception e1) {
			LOG.error("Something went wrong during guard size calculation: " + e1.getMessage());
			e1.printStackTrace();
		}

		return totalBytes * 3;

	}



}
