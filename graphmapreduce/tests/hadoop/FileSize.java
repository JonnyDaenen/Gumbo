/**
 * Created: 14 Jan 2015
 */
package hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author jonny
 *
 */
public class FileSize {


	public static void main(String[] args) {
		try {
			System.out.println(getflSize("input/experiments/EXP_007/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * source: http://stackoverflow.com/a/22485418/787036
	 */
	public static long getflSize(String args) throws IOException, FileNotFoundException
	{
		Configuration config = new Configuration();
		Path path = new Path(args);
		FileSystem hdfs = path.getFileSystem(config);
		ContentSummary cSummary = hdfs.getContentSummary(path);
		long length = cSummary.getLength();
		return length;
	}


}
