package gumbo.engine.hadoop2.mapreduce.semijoin;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleFilter;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleProjection;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class MultiSemiJoinMapper extends Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable> {

	protected VBytesWritable bw;
	protected GumboMessageWritable gw;
	
	// intermediate key buffers
	private VLongWritable lw1;
	private VLongWritable lw2;
	private long offset;
	
	private QuickWrappedTuple qt;
	private TupleProjection [] projections;
	private TupleFilter[] filters;
	private DataOutputBuffer buffer;


	@Override
	protected void setup(Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		// buffers
		qt = new QuickWrappedTuple();
		bw = new VBytesWritable();
		gw = new GumboMessageWritable();


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
		projections = TupleOpFactory.createMapMSJProjections(relation, fileid, queries, atomidmap);



	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {


		// create QuickTuple
		qt.initialize(value);


		// ASSERTS & DATA
		// for each guard atom
		for (int i = 0; i < projections.length; i++) {

			TupleProjection pi = projections[i];

			// if matches. store the output in writables
			if (pi.load(qt, key.get(), bw, gw)){

				// and write output
				context.write(bw, gw);
			}

		}


	}

}
