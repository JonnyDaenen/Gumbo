/**
 * Created: 28 Jan 2015
 */
package gumbo.utils.estimation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Used to estimate the number of tuple in a file.
 * @author Jonny Daenen
 *
 */
public abstract class TupleEstimator {
	

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(TupleEstimator.class); 
	protected long defaultValue = 1;
	
	
	abstract public long estimateNumTuples(Path path);
	
	
	
	
	
	/**
	 * Samples a file at several offsets and returns an estimate for the number of tuples.
	 * 
	 * @param path the file path
	 * @param blockSize the size of each sample, in bytes
	 * @param offsets a list of offsets to sample at
	 * 
	 * @return an estimate for the number of tuples in the file.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected long estimateNumTuples(Path path, int blockSize, long [] offsets) throws FileNotFoundException, IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = null;
	
		long size = Sampler.getFileSize(path);
		try {
			
			
			in = fs.open(path); 
			
			
			long total = 0;
			for (int i = 0; i < offsets.length; i++) {
				
				long start = offsets[i];
				total += Sampler.sampleBlock(in, start, start+blockSize, size);
			}
			

			return (long) Math.ceil(total * (size / (double)(offsets.length * blockSize)));

		} finally { 
			IOUtils.closeStream(in);
		}
		
//		return size / 128;
	}
}
