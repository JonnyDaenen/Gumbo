package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VLongPair;

public class EvaluateMapper extends Mapper<LongWritable, Text, VLongPair, GumboMessageWritable> {

	
	private VLongPair lw;
	private GumboMessageWritable gw;

	@Override
	protected void setup(Mapper<LongWritable, Text, VLongPair, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		lw = new VLongPair();
		gw = new GumboMessageWritable();
	
		// TODO determine file id
		long fileid = 0;
		lw.setFirst(fileid);
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, VLongPair, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {
		
		// TODO apply tuple check: should match at least one
		
		lw.setSecond(key.get());
		gw.setContent(value.getBytes(), value.getLength());
		
		context.write(lw, gw);
		
	}
}
