package gumbo.engine.hadoop2.mapreduce.multivalidate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.GumboCounters;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.AssertRequestPacker;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleProjection;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.Pair;

public class ValidateMapper extends Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable> {


	private QuickWrappedTuple qt;
	private VBytesWritable bw;
	private GumboMessageWritable gw;
	private TupleProjection [] projections;

	private boolean packingEnabled;
	private AssertRequestPacker packer;
	
	private long numAsserts;
	private long numRequests;
	private long numIn;


	@Override
	public void setup(Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		// buffers
		qt = new QuickWrappedTuple();
		bw = new VBytesWritable();
		gw = new GumboMessageWritable();

		packer = new AssertRequestPacker(10);

		ContextInspector inspector = new ContextInspector(context);

		// get fileid
		long fileid = inspector.getFileId();

		// get relation
		String relation = inspector.getRelationName(fileid);

		// get queries
		Set<GFExistentialExpression> queries = inspector.getQueries();

		// get atom id mapping
		Map<String, Integer> atomidmap = inspector.getAtomIdMap();

		// get projections
		boolean merge = inspector.isProjectionMergeEnabled();
		projections = TupleOpFactory.createMap1Projections(relation, fileid, queries, atomidmap, merge);

		// enable packing
		packingEnabled = true;

		if (projections.length == 1) {
			packingEnabled = false;
		}

		// OPTIMIZE disable packing if there are no key implications
		
		// counters
		numAsserts = 0;
		numRequests = 0;
		numIn = 0;
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);

		context.getCounter(GumboCounters.RECORDS_IN).increment(numIn);
		
		context.getCounter(GumboCounters.ASSERT_OUT).increment(numAsserts);
		context.getCounter(GumboCounters.REQUEST_OUT).increment(numRequests);
	}


	@Override
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {

		// create QuickTuple
		qt.initialize(value);
		numIn++;

		// for each guard atom
		for (int i = 0; i < projections.length; i++) {

			TupleProjection pi = projections[i];

			// if matches. store the output in writables
			if (pi.load(qt, key.get(), bw, gw)){

				if (gw.isAssert()) {
					numAsserts++;
				} else {
					numRequests++;
				}
				
				// if packing is disabled, write output directly
				if (!packingEnabled) {
					context.write(bw, gw);
					// otherwise, buffer
				} else
					packer.add(bw, gw);

			}

		}

		if (packingEnabled) {
			// cross-message packing
			for (Pair<VBytesWritable, GumboMessageWritable> kvpair : packer.pack()) {
				context.write(kvpair.fst, kvpair.snd);
			}
			packer.clear();
		}

	}

}
