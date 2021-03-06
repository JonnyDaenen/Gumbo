package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.GumboCounters;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.ConfirmBuffer;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class EvaluateReducer extends Reducer<VBytesWritable, GumboMessageWritable, Text, Text> {

	private Text output;
	
	private ConfirmBuffer buffer;
	private TupleEvaluator [] projections;

	private MultipleOutputs<Text, Text> mos;

	private long numData;
	private long numConfirm;
	private long numOut;
	
	@Override
	protected void setup(Reducer<VBytesWritable, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		output = new Text();

		mos = new MultipleOutputs<>(context);
		
		
		ContextInspector inspector = new ContextInspector(context);
		
		// get queries
		Set<GFExistentialExpression> queries = inspector.getQueries();
		
		// get mapping
		Map<String, Integer> atomidmap = inspector.getAtomIdMap();
		
		// get projections
		projections = TupleOpFactory.createRed2Projections(queries, inspector.getOutMapping(), atomidmap);
		
		// create buffer
		int maxatomid = inspector.getMaxAtomID();
		buffer = new ConfirmBuffer(maxatomid);
		
		// counters
		numData = 0;
		numConfirm = 0;
		numOut = 0;
		
	}
	
	@Override
	protected void cleanup(Reducer<VBytesWritable, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
		
		context.getCounter(GumboCounters.DATA_IN).increment(numData);
		context.getCounter(GumboCounters.CONFIRM_IN).increment(numConfirm);
		
		context.getCounter(GumboCounters.RECORDS_OUT).increment(numOut);
	}
	
	@Override
	protected void reduce(VBytesWritable key, Iterable<GumboMessageWritable> values,
			Reducer<VBytesWritable, GumboMessageWritable, Text, Text>.Context context)
					throws IOException, InterruptedException {
		
		
		for (GumboMessageWritable value : values) {
		
			// extract data message
			if (value.isData()){
				buffer.setMessage(value);
				numData++;
			// keep track of confirmed atoms
			} else {
				buffer.addAtomIDs(value);
				numConfirm++;
			}
		}
		
		// TODO check if data value is missing
		for (TupleEvaluator pi : projections) {
			if (buffer.load(pi, output)) {
				mos.write((Text)null, output, pi.getFilename());
				numOut++;
			}
		}
		

		buffer.reset();
		
	}
}
