package gumbo.engine.hadoop2.mapreduce.multivalidate;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import gumbo.engine.hadoop.mrcomponents.round1.reducers.GFReducer1Optimized;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.RequestBuffer;

public class ValidateReducer extends Reducer<BytesWritable, GumboMessageWritable, BytesWritable, GumboMessageWritable> {

	private static final Log LOG = LogFactory.getLog(GFReducer1Optimized.class);


	private GumboMessageWritable gw;
	private BytesWritable bw;

	private RequestBuffer buffer;



	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		// load context
		super.setup(context);

		gw = new GumboMessageWritable();
		bw = new BytesWritable();
		
		// get max atom id
		
		ContextInspector ci = new ContextInspector(context);
		int maxAtomID = ci.getMaxAtomID();
		
		buffer = new RequestBuffer(maxAtomID);
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
	
	@Override
	protected void reduce(BytesWritable key, Iterable<GumboMessageWritable> values,
			Reducer<BytesWritable, GumboMessageWritable, BytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {
		

		buffer.reset();
		
		// consider all incoming messages
		for (GumboMessageWritable value : values) {
			
			// check atoms ids that are present
			if (value.isAssert()) {
				buffer.addAtomIds(value);
				
			// buffer requests
			} else {
				buffer.addMessage(value);
			}
			
		}
		
		// for all buffered messages
		for (int i = 0; i < buffer.size(); i++) {
			
			// process and output them if necessary
			if (buffer.load(i, bw, gw)) {
				context.write(bw, gw);
			}
		}
		
		
	}

	



}
