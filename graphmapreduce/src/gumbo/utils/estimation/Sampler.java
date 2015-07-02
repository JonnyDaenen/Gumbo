package gumbo.utils.estimation;

import gumbo.structures.gfexpressions.io.Pair;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class Sampler {


	private static final Log LOG = LogFactory.getLog(Sampler.class); 





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



	private static double sampleBlock(FSDataInputStream in, long start, long end, long fileSize, Function<byte[], Double> f) throws SamplingException {

		try {
			// adjust bounds
			start = Math.max(0,start);
			end = Math.min(end, fileSize);
			int amount = (int)(end - start);

			byte [] buf = new byte[amount];
			in.seek(start);
			IOUtils.readFully(in, buf, 0 , amount);
			//			in.seek(0); // not necessary


			return f.apply(buf);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new SamplingException("Something went wrong during sampling.", e);
		}

	}

	private static double sampleBlocks(Path path, long blockSize, long [] offsets, Function<byte[], Double> map, Function<Pair<Double,Double>, Double> reduce) throws SamplingException {


		FSDataInputStream in = null;

		try {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			long size = Sampler.getFileSize(path);
			double [] results = new double[offsets.length];


			in = fs.open(path); 


			long total = 0;
			for (int i = 0; i < offsets.length; i++) {

				long start = offsets[i];
				results[i] = Sampler.sampleBlock(in, start, start+blockSize, size, map);
			}


			double finalVal = results[0];
			for (int i = 1; i < offsets.length; i++) {
				Pair<Double, Double> p = new Pair<Double, Double>(finalVal, results[i]);
				finalVal = reduce.apply(p);
			}

			return finalVal;

		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new SamplingException("Something went wrong during sampling.", e);
		} finally { 
			IOUtils.closeStream(in);
		}

		//		return size / 128;
	}

	private static double sampleRandomBlocks(Path path, int numSamples, long blockSize, Function<byte[], Double> map, Function<Pair<Double, Double>, Double> reduce) throws SamplingException {
		try {
			long size = Sampler.getFileSize(path);

			long [] offsets = new long[numSamples];
			for (int i = 0; i < offsets.length; i++)
				offsets[i] = Sampler.getRandom(0,size-blockSize);

			return sampleBlocks(path, blockSize, offsets, map, reduce);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new SamplingException("Something went wrong during sampling.", e);
		}
	}

	private static double sampleRandomPaths(Set<Path> paths, int numSamples, long blockSize, Function<byte[], Double> map, Function<Pair<Double, Double>, Double> reduce) throws SamplingException {


		double [] results = new double[paths.size()];

		int i = 0;
		for (Path path : paths) {
			results[i++] = sampleRandomBlocks(path, numSamples, blockSize, map, reduce);
		}

		double finalVal = results[0];
		for (i = 1; i < results.length; i++) {
			Pair<Double, Double> p = new Pair<Double, Double>(finalVal, results[i]);
			finalVal = reduce.apply(p);
		}

		return finalVal;
	}

	
	/**
	 * 
	 * @param path
	 * @param numBlocks
	 * @param blockSize
	 * @return
	 * @throws SamplingException
	 */
	public static byte [] [] getRandomBlocks(Path path, int numBlocks, long blockSize) throws SamplingException {
		try {
			long size = Sampler.getFileSize(path);

			long [] offsets = new long[numBlocks];
			for (int i = 0; i < offsets.length; i++)
				offsets[i] = Sampler.getRandom(0,Math.max(size,Math.max(0,size-blockSize)));
			
			Arrays.sort(offsets);

			return getBlocks(path, blockSize, offsets, size);
		} catch (IOException e) {
			throw new SamplingException("Something went wrong during sampling.", e);
		}
	}

	private static byte[][] getBlocks(Path path, long blockSize, long[] offsets, long filesize) throws SamplingException {

		FSDataInputStream in = null;
		
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			in = fs.open(path); 
			
			byte [] [] result = new byte[offsets.length][];


			for (int i = 0; i < offsets.length; i++) {

				// adjust bounds
				long start = Math.max(0,offsets[i]);
				long end = Math.min(offsets[i]+blockSize,filesize);
				int amount = (int)(end - start);

				byte [] buf = new byte[amount];
				in.seek(start);
				IOUtils.readFully(in, buf, 0 , amount);
				//			in.seek(0); // not necessary
				result[i] = buf;


			}
			return result;
		} catch (Exception e) {
			throw new SamplingException("Something went wrong during sampling.", e);

		} finally { 
			IOUtils.closeStream(in);
		}
	}





}
