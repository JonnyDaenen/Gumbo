/**
 * Created: 08 Oct 2014
 */
package gumbo.engine.hadoop.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;


/**
 * @author jonny
 *
 */
public class GuardRecordReaderFast extends RecordReader<Text, IntWritable> {

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(GuardRecordReaderFast.class);

    private IntWritable value = new IntWritable();
    KeyValueLineRecordReader reader;
    
    /**
     * @param configuration 
	 * 
	 */
	public GuardRecordReaderFast(Configuration configuration) {
		try {
			reader = new KeyValueLineRecordReader(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		reader.initialize(split, context);

	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
		Text help = reader.getCurrentValue();
		int i = Integer.parseInt(help.toString());
//		LOG.warn("test: " + i);
		value.set(i);
		return value;
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	@Override
	public void close() throws IOException {
		reader.close();

	}

}
