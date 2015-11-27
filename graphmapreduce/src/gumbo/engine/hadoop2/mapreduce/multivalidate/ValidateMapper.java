package gumbo.engine.hadoop2.mapreduce.multivalidate;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.hadoop2.datastructures.QuickWrappedTuple;
import gumbo.engine.hadoop2.datastructures.TupleProjection;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.structures.data.QuickTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class ValidateMapper extends Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable> {

	
	private QuickWrappedTuple qt;
	private BytesWritable bw;
	private GumboMessageWritable gw;
	private TupleProjection [] projections;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		// buffers
		qt = new QuickWrappedTuple();
		bw = new BytesWritable();
		gw = new GumboMessageWritable();
		
		// get filename
		
		// get relation
		
		// get atoms
		
		// get projections
		
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, BytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {
		
		// create QuickTuple
		qt.initialize(value);
		
		// for each guard atom
		for (int i = 0; i < projections.length; i++) {
			
			TupleProjection pi = projections[i];
			
			// if matches
			if (pi.matches(qt)){
				
				// project to message
				pi.project(qt, bw, gw);
		
				// send output
				context.write(bw, gw);
			}
			
		}
		
		
	}
	
}
