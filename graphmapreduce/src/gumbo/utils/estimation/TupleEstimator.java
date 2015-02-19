/**
 * Created: 28 Jan 2015
 */
package gumbo.utils.estimation;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
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
	
		long size = getFileSize(path);
		try {
			
			
			in = fs.open(path); 
			
			
			long total = 0;
			for (int i = 0; i < offsets.length; i++) {
				
				long start = offsets[i];
				total += sampleBlock(in, start, start+blockSize, size);
			}
			

			return (long) Math.ceil(total * (size / (double)(offsets.length * blockSize)));

		} finally { 
			IOUtils.closeStream(in);
		}
		
//		return size / 128;
	}
	
	
	/**
	 * Counts the number of newlines in a a block starting at start and ending at end (exclusive).
	 * @return
	 */
	protected static long sampleBlock(FSDataInputStream in, long start, long end, long fileSize) {
		
		try {
			// adjust bounds
			start = Math.max(0,start);
			end = Math.min(end, fileSize);
			int amount = (int)(end - start);
			
			byte [] buf = new byte[amount];
			in.seek(start);
			IOUtils.readFully(in, buf, 0 , amount);
//			in.seek(0); // not necessary
			

			return countNewlines(buf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return (end - start) / 128;
	}
	
	/**
	 * Counts the number of newline symbols appearing in a byte buffer.
	 * @param buf the buffer.
	 * @return the number of newlines in the buffer
	 */
	protected static int countNewlines(byte[] buf) {
		int lines = 0;
		for (int i = 0; i < buf.length; i++) {
			if (buf[i] == '\n')
				lines++;

		}
		return lines;
	}

	// TODO move to utility class
	public static long getRandom(long min, long max) {
		return (long) (Math.random() * (max - min + 1) + min);
	}
	

	/**
	 * source: http://stackoverflow.com/a/22485418/787036
	 */
	public static long getFileSize(Path path) throws IOException, FileNotFoundException
	{
		Configuration config = new Configuration();
		FileSystem hdfs = path.getFileSystem(config);
		ContentSummary cSummary = hdfs.getContentSummary(path);
		long length = cSummary.getLength();
		return length;
	}
}
