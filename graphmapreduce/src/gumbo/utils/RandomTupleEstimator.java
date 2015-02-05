/**
 * Created: 28 Jan 2015
 */
package gumbo.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * Samples a file at a specified number of random locations.
 * The number of newline characters that appear are then counted 
 * and used to estimate the number of tuples in the total file.
 * 
 * @author Jonny Daenen
 *
 */
public class RandomTupleEstimator extends TupleEstimator {


	private static final Log LOG = LogFactory.getLog(RandomTupleEstimator.class); 

	int blockSize;
	int numSamples;


	public RandomTupleEstimator() {
		this(1024, 5);
	}

	public RandomTupleEstimator(int blockSize, int numSamples) {
		this.blockSize = blockSize;
		this.numSamples = numSamples;
	}


	/**
	 * Estimates the number of tuples by sampling 3 points in the file.
	 * @return an estimate for the number of tuples, 1 if something goes wrong.
	 * @see gumbo.utils.TupleEstimator#estimateNumTuples(org.apache.hadoop.fs.Path)
	 */
	@Override
	public long estimateNumTuples(Path path) {
		long size;
		try {
			size = getFileSize(path);
			
			long [] offsets = new long[numSamples];
			for (int i = 0; i < offsets.length; i++)
				offsets[i] = getRandom(0,size-blockSize);

			return estimateNumTuples(path, blockSize, offsets);
			
		} catch (IOException e) {
			LOG.warn("Could not estimate number of tuples in " + path + ", using default value " + defaultValue + "." );
			e.printStackTrace();
			return defaultValue;
		}


	}

}
