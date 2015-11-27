package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VLongPair;
import gumbo.engine.hadoop2.mapreduce.tools.ConfirmBuffer;
import gumbo.engine.hadoop2.mapreduce.tools.TupleProjection;

public class EvaluateReducer extends Reducer<VLongPair, GumboMessageWritable, Text, Text> {

	
	private ConfirmBuffer buffer;
	private Object projections;

	@Override
	protected void setup(Reducer<VLongPair, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		// TODO get relation name
		// TODO get filename
		// TODO get bsgf queries for this guard
		// TODO get projections
		
	}
	
	@Override
	protected void reduce(VLongPair key, Iterable<GumboMessageWritable> values,
			Reducer<VLongPair, GumboMessageWritable, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		for (GumboMessageWritable value : values) {
			
			if (value.isData()){
				buffer.addMessage(value);
			} else {
				buffer.addAtomIDs(value);
			}
		}
		
		for (TupleProjection pi : projections) {
			if (buffer.eval(pi)) {
				mos.write(NULL, output, pi.getFilename());
			}
		}
		
	}
}
