/**
 * Created: 28 Jan 2015
 */
package hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author jonny
 *
 */
public class FileSeek {

	private static final int NUM = 3;
	private static final long BLOCK_SIZE = 2*1024;

	public static void main(String[] args) throws FileNotFoundException, IOException {


		String filename = "input/q4/1e04/R_6e04x4e00_func-seqclone.rel";

		System.out.println("Actual avg tuple size:" + actualAvgTupleSize(filename) );
		System.out.println("Actual number of tuples:" + actualNumTuples(filename));

		long size = FileSize.getflSize(filename);
		System.out.println("File size: " + size);


		System.out.println("Estimated num Tuples: " + estimateNumTuples(filename));
		System.out.println("Estimated num Tuples: " + estimateNumTuplesRandom(filename));


	}

	/**
	 * @param filename
	 * @return
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private static double estimateNumTuples(String filename) throws FileNotFoundException, IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = null;
	
		long size = FileSize.getflSize(filename);
		try {
			
			
			in = fs.open(new Path(filename)); 
			
			
			long begin = sampleBlock(in, 0,BLOCK_SIZE, size);
			long mid = sampleBlock(in, size/2,size/2 + BLOCK_SIZE, size); 
			long end = sampleBlock(in, size - BLOCK_SIZE, size, size);
			

			return (begin + mid + end) * (size / (double)(3 * BLOCK_SIZE));

		} finally { 
			IOUtils.closeStream(in);
		}
		
//		return size / 128;
	}
	
	private static double estimateNumTuplesRandom(String filename) throws FileNotFoundException, IOException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = null;
	
		long size = FileSize.getflSize(filename);
		try {
			
			
			in = fs.open(new Path(filename)); 
			
			
			long total = 0;
			for (int i = 0; i < NUM; i++) {
				
				long start = getRandom(0,size-BLOCK_SIZE);
				total += sampleBlock(in, start, start+BLOCK_SIZE, size);
			}
			

			return total * (size / (double)(NUM * BLOCK_SIZE));

		} finally { 
			IOUtils.closeStream(in);
		}
		
//		return size / 128;
	}

	/**
	 * Random number in [x,y]
	 * @param min the minimum (incl)
	 * @param max the maximum (incl)
	 * @return
	 */
	private static long getRandom(long min, long max) {
		return (long) (Math.random() * (max - min + 1) + min);
	}

	/**
	 * Counts the number of newlines in a a block starting at start and ending at end (exclusive).
	 * @return
	 */
	private static long sampleBlock(FSDataInputStream in, long start, long end, long fileSize) {
		
		try {
			// adjust bounds
			start = Math.max(0,start);
			end = Math.min(end, fileSize);
			int amount = (int)(end - start);
			
			byte [] buf = new byte[amount];
			
//			System.out.println("Reading [" + start + "," + end + "] = " + amount ); 
			in.seek(start);
			IOUtils.readFully(in, buf, 0 , amount);
//			in.seek(0);
			
//			System.out.println(new String(buf));

			return countNewlines(buf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return (end - start) / 128;
	}

	/**
	 * @param filename
	 * @return
	 * @throws FileNotFoundException 
	 */
	private static int actualNumTuples(String path) throws FileNotFoundException {
		int counter = 0;
		Scanner input = new Scanner(new File(path));
		while (input.hasNextLine()) {
			input.nextLine();
			counter++;
		}
		input.close();
		return counter;
	}

	/**
	 * @param filename
	 * @return
	 * @throws FileNotFoundException 
	 */
	private static double actualAvgTupleSize(String path) throws FileNotFoundException {
		int counter = 0;
		int bytes = 0;
		Scanner input = new Scanner(new File(path));
		while (input.hasNextLine()) {
			String line = input.nextLine();
			bytes += line.length();
			counter++;
		}
		input.close();
		return bytes/(double)counter;
	}

	/**
	 * @param buf
	 * @return
	 */
	private static int countNewlines(byte[] buf) {
		int lines = 0;
		for (int i = 0; i < buf.length; i++) {
			if (buf[i] == '\n')
				lines++;

		}
		return lines;
	}

}


//// OLD:
//IOUtils.copyBytes(in, System.out, 8*8L, false); 
//System.out.println("done."); 
//
//in.seek(byteOffset); // go back to the start of the file 
//IOUtils.copyBytes(in, System.out, 8*8L, false);
//System.out.println("done."); 
//
//byte [] buf = new byte[64];
//IOUtils.readFully(in, buf, 0, 54);
//System.out.println(new String(buf));
//System.out.println("done."); 
//
//int nls = countNewlines(buf);
//System.out.println("newlines: " + nls);
