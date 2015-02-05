package gumbo.executor.hadoop.extensions;
/**
 * Created: 17 Dec 2014
 */

/**
 * @author jonny
 *
 */

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Identical to TextOutputFormat, only default separator is empty instead of tab.
 * @author Jonny Daenen
 *
 * @param <K>
 * @param <V>
 */
public class CustomOutputFormat<K, V> extends TextOutputFormat<K, V> {
	
	/**
	 * @see org.apache.hadoop.mapreduce.lib.output.TextOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
	    boolean isCompressed = getCompressOutput(job);
	    String keyValueSeparator= conf.get(SEPERATOR, ""); // replaced "\t" with ""
	    CompressionCodec codec = null;
	    String extension = "";
	    if (isCompressed) {
	      Class<? extends CompressionCodec> codecClass = 
	        getOutputCompressorClass(job, GzipCodec.class);
	      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	      extension = codec.getDefaultExtension();
	    }
	    Path file = getDefaultWorkFile(job, extension);
	    FileSystem fs = file.getFileSystem(conf);
	    if (!isCompressed) {
	      FSDataOutputStream fileOut = fs.create(file, false);
	      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
	    } else {
	      FSDataOutputStream fileOut = fs.create(file, false);
	      return new LineRecordWriter<K, V>(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),
	                                        keyValueSeparator);
	    }
	}

    
}