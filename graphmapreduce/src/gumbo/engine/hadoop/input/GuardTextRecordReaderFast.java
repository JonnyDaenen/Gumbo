/**
 * Created: 08 Oct 2014
 */
package gumbo.engine.hadoop.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;


/**
 * This class should be replaced by KeyValueLineRecordReader.
 * 
 * @author Jonny Daenen
 *
 */
public class GuardTextRecordReaderFast extends RecordReader<Text, Text> {

	private static final Log LOG = LogFactory.getLog(GuardTextRecordReaderFast.class);

    private Text value = new Text();
    KeyValueLineRecordReader reader;
    
    /**
     * @param configuration 
	 * 
	 */
	public GuardTextRecordReaderFast(Configuration configuration) {
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
	public Text getCurrentValue() throws IOException, InterruptedException {
		Text help = reader.getCurrentValue();
//		LOG.warn("test: " + i);
		value.set(help);
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
