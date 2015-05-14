/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper2GuardRel extends GFMapper2Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper2GuardRel.class);

	IntWritable out1 = new IntWritable();
	Text out2 = new Text();

	private TupleIDCreator pathids;

	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper2Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(rm);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		value.set(value.toString().trim());

		try {




			Tuple t = new Tuple(value);
			// System.out.println(t);

			// replace value with pointer when optimization is on
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardTuplePointerOptimizationOn)) {
				value.set(pathids.getTupleID(context, key.get())); // key indicates offset in TextInputFormat
			}


			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guardid
					out1.set(guardID);
					context.write(value, out1);
					context.getCounter(GumboMap2Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
					context.getCounter(GumboMap2Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(Integer.SIZE/8 + value.getLength());
					
					// output tuple value
					// only when pointer optimization is on
					// to be able to recover the tuple in the reducer
					if (settings.getBooleanProperty(HadoopExecutorSettings.guardTuplePointerOptimizationOn)) {
						out2.set(t.toString());
//						context.write(value, out2); // FIXME change output to text... :-(
						context.getCounter(GumboMap2Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
						context.getCounter(GumboMap2Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(out2.getLength() + value.getLength());
					}




				}
			}

		} catch (SecurityException | GFOperationInitException | TupleIDError e) {
			// should not happen!
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

}
