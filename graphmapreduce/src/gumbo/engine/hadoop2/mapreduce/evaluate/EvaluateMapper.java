package gumbo.engine.hadoop2.mapreduce.evaluate;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable.GumboMessageType;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleFilter;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class EvaluateMapper extends Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable> {


	private BytesWritable bw;
	private GumboMessageWritable gw;
	
	// intermediate key buffers
	private VLongWritable lw1;
	private VLongWritable lw2;
	private long offset;
	
	private QuickWrappedTuple qt;
	private TupleFilter[] filters;
	private DataOutputBuffer buffer;

	@Override
	protected void setup(Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		bw = new BytesWritable();
		gw = new GumboMessageWritable();
		gw.setType(GumboMessageType.DATA);
		qt = new QuickWrappedTuple();


		// buffer for output key bytes
		buffer = new DataOutputBuffer(32);

		ContextInspector inspector = new ContextInspector(context);


		// get fileid
		long fileid = inspector.getFileId();
		lw1 = new VLongWritable();
		lw2 = new VLongWritable();
		lw1.set(fileid);

		// get relation
		String relation = inspector.getRelationName(fileid);
		
		// get guard atoms
		Set<GFAtomicExpression> atoms = inspector.getGuardAtoms();

		// create filter
		filters = TupleOpFactory.createMap2Filter(atoms, relation);
		
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {


		qt.initialize(value);
		
		// tuple should match at least one guard atom
		boolean match = false;
		for (TupleFilter filter : filters) {
			if (filter.check(qt)) {
				match = true;
				break;
			}
		}
		
		if (!match)
			return;
		
		
		// prepare key bytes
		offset = key.get();
		lw2.set(offset);
		
		buffer.reset();
		lw1.write(buffer);
		lw2.write(buffer);
		byte[] data = buffer.getData();
		int dataLength = buffer.getLength();
		bw.set(data, 0, dataLength);
		
		// prepare message
		// OPTIMIZE cut out irrelevant attributes
		gw.setDataBytes(value.getBytes(), value.getLength());

		// write to output
		context.write(bw, gw);

	}
}
