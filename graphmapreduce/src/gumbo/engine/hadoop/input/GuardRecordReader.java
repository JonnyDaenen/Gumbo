/**
 * Created: 08 Oct 2014
 */
package gumbo.engine.hadoop.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * @author jonny
 *
 */
public class GuardRecordReader extends RecordReader<Text, IntWritable> {
	

    private IntWritable value = new IntWritable();
    private Text key = new Text();
    LineRecordReader reader;
    
    /**
     * @param configuration 
	 * 
	 */
	public GuardRecordReader(Configuration configuration) {
		reader = new LineRecordReader();
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
		if (!reader.nextKeyValue())
			return false;
		
		Text val = reader.getCurrentValue();
		String[] vals = val.toString().split("\t");
		
		if (vals.length == 2) {
			key.set(vals[0]);
			value.set(Integer.parseInt(vals[1]));
		} else if (vals.length == 1) {
			// TODO skip?
			throw new InterruptedException("Missing value!");
		} else {
			throw new InterruptedException("Missing values!");
		}
		
		
		return true;
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	/**
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public IntWritable getCurrentValue() throws IOException, InterruptedException {
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
