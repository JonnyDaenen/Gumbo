/**
 * Created: 25 Apr 2014
 */
package mapreduce.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;

/**
 * Find out the tmp dir of current hadoop instance.
 * This is NOT the HDFS tmp dir
 * 
 * @author Jonny Daenen
 *
 */
public class TmpDir extends Configured {
	
	public static void main(String[] args) throws IOException {
		TmpDir test = new TmpDir();
		test.execute();
	}
	
	public void execute() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String dir = conf.get("hadoop.tmp.dir");
		
		System.out.println(dir);
	}

}
