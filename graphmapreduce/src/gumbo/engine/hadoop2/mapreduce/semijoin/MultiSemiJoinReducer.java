package gumbo.engine.hadoop2.mapreduce.semijoin;

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
import gumbo.engine.hadoop2.mapreduce.tools.buffers.RecyclableBuffer;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class MultiSemiJoinReducer  extends Reducer<VBytesWritable, GumboMessageWritable, Text, Text> {

	private Text output;

	private ConfirmBuffer buffer;
	private RecyclableBuffer<GumboMessageWritable> dmbuffer; // TODO check if buffer is actually better
	private TupleEvaluator [] projections;

	private MultipleOutputs<Text, Text> mos;

	private long numAsserts;
	private long numData;
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
		dmbuffer = new RecyclableBuffer<>(16);


	}
	

	@Override
	protected void cleanup(Reducer<VBytesWritable, GumboMessageWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		mos.close();
		
		context.getCounter(GumboCounters.ASSERT_IN).increment(numAsserts);
		context.getCounter(GumboCounters.DATA_IN).increment(numData);
		context.getCounter(GumboCounters.RECORDS_OUT).increment(numOut);
	}

	@Override
	protected void reduce(VBytesWritable key, Iterable<GumboMessageWritable> values,
			Reducer<VBytesWritable, GumboMessageWritable, Text, Text>.Context context)
					throws IOException, InterruptedException {



		for (GumboMessageWritable value : values) {

			// buffer the data objects
			if (value.isData()){
				dmbuffer.addWritable(value); // TODO make it byteswritable
				numData++;

				// keep track of ASSERTED atoms
			} else if (value.isAssert()){
				buffer.addAtomIDs(value);
				numAsserts++;
			}
		}

		// TODO check if data value is missing
		for (TupleEvaluator pi : projections) {

			// for each cached message
			for (int i = 0; i < dmbuffer.size(); i++) {
				GumboMessageWritable dm = dmbuffer.get(i);
				buffer.setMessage(dm);
				
				if (buffer.load(pi, output)) {
					mos.write((Text)null, output, pi.getFilename());
					numOut++;
				}
			}
		}
		
		buffer.reset();
		dmbuffer.clear();
		

	}

}
