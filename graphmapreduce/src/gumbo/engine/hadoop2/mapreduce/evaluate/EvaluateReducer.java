package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VLongPair;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.ConfirmBuffer;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleProjection;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class EvaluateReducer extends Reducer<VLongPair, GumboMessageWritable, Text, Text> {

	private Text output;
	
	private ConfirmBuffer buffer;
	private TupleEvaluator [] projections;

	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void setup(Reducer<VLongPair, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		output = new Text();

		mos = new MultipleOutputs<>(context);
		
		
		ContextInspector inspector = new ContextInspector(context);
		
		// get queries
		Set<GFExistentialExpression> queries = inspector.getQueries();
		
		// get projections
		projections = TupleOpFactory.createRed2Projections(queries);
		
		
	}
	
	@Override
	protected void cleanup(Reducer<VLongPair, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
	}
	
	@Override
	protected void reduce(VLongPair key, Iterable<GumboMessageWritable> values,
			Reducer<VLongPair, GumboMessageWritable, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		
		buffer.reset();
		
		for (GumboMessageWritable value : values) {
		
			// extract data message
			if (value.isData()){
				buffer.setMessage(value);
			
			// keep track of confirmed atoms
			} else {
				buffer.addAtomIDs(value);
			}
		}
		
		for (TupleEvaluator pi : projections) {
			if (buffer.load(pi, output)) {
				mos.write((Text)null, output, pi.getFilename());
			}
		}
		
	}
}
