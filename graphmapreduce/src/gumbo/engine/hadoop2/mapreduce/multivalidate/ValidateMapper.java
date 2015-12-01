package gumbo.engine.hadoop2.mapreduce.multivalidate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleOpFactory;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleProjection;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class ValidateMapper extends Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable> {

	
	private QuickWrappedTuple qt;
	private VBytesWritable bw;
	private GumboMessageWritable gw;
	private TupleProjection [] projections;
	
	
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
		projections = TupleOpFactory.createMap1Projections(relation, fileid, queries, atomidmap);
		
	}
	

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {
		
		// create QuickTuple
		qt.initialize(value);
		
		// for each guard atom
		for (int i = 0; i < projections.length; i++) {
			
			TupleProjection pi = projections[i];
			
			// if matches. store the output in writables
			if (pi.load(qt, key.get(), bw, gw)){
				
				// and write output
				context.write(bw, gw);
			}
			
		}
		
		// TODO implement over-message packing
		
		
	}
	
}
