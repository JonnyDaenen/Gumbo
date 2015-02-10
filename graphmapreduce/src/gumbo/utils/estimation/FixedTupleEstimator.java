/**
 * Created: 28 Jan 2015
 */
package gumbo.utils.estimation;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * Samples a file at 3 locations: start, mid and end.
 * The number of newline characters that appear are then counted 
 * and used to estimate the number of tuples in the total file.
 * 
 * @author Jonny Daenen
 *
 */
public class FixedTupleEstimator extends TupleEstimator {
	
	private static final Log LOG = LogFactory.getLog(FixedTupleEstimator.class); 

	
	int blockSize;
	
	
	public FixedTupleEstimator() {
		this(1024);
	}
	
	public FixedTupleEstimator(int blockSize) {
		this.blockSize = blockSize;
	}
	
	
	/**
	 * Estimates the number of tuples by sampling 3 points in the file.
	 * @return an estimate for the number of tuples, 1 if something goes wrong.
	 * @see gumbo.utils.estimation.TupleEstimator#estimateNumTuples(org.apache.hadoop.fs.Path)
	 */
	@Override
	public long estimateNumTuples(Path path) {
		long size;
		try {
			size = getFileSize(path);
			long [] offsets = new long[3];
			offsets[0] = 0;
			offsets[1] = size / 2;
			offsets[2] = size - blockSize;
			
			return estimateNumTuples(path, blockSize, offsets);
		} catch (IOException e) {
			LOG.warn("Could not estimate number of tuples in " + path + ", using default value " + defaultValue + "." );
			e.printStackTrace();
			return defaultValue;
		}
		
		
	}

}
