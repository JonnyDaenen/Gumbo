package gumbo.engine.hadoop2.mapreduce.multivalidate;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import gumbo.engine.hadoop.mrcomponents.round1.reducers.GFReducer1Optimized;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.ContextInspector;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.RequestBuffer;

public class ValidateReducer extends Reducer<VBytesWritable, GumboMessageWritable, VBytesWritable, GumboMessageWritable> {

	private static final Log LOG = LogFactory.getLog(GFReducer1Optimized.class);


	private GumboMessageWritable gw;
	private VBytesWritable bw;

	private RequestBuffer buffer;


	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		// load context
		super.setup(context);

		gw = new GumboMessageWritable();
		bw = new VBytesWritable();
		
		// get max atom id
		
		ContextInspector ci = new ContextInspector(context);
		int maxAtomID = ci.getMaxAtomID();
		
		buffer = new RequestBuffer(maxAtomID);
	
	}

	
	@Override
	protected void reduce(VBytesWritable key, Iterable<GumboMessageWritable> values,
			Reducer<VBytesWritable, GumboMessageWritable, VBytesWritable, GumboMessageWritable>.Context context)
					throws IOException, InterruptedException {
		

		buffer.reset();

		System.out.println("input key:\t" + key);
		
		// consider all incoming messages
		for (GumboMessageWritable value : values) {
			System.out.println("input val:\t" + value);
			
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
//				System.out.println("key:\t" + bw);
//				System.out.println("val:\t" + gw);
			}
		}
		
		
	}

	



}
