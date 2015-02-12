/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.compiler.structures.data.Tuple;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.ExecutorSettings;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper2GuardTextRel extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper2GuardTextRel.class);

	Text out1 = new Text();
	Text out2 = new Text();

	private TupleIDCreator pathids;

	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper2Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(eso.getFileMapping());
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		value.set(value.toString().trim());
		
		boolean print = false;
		if (value.toString().contains(",1000,")) {
			LOG.error("Mapper1: " + value);
			print = true;
		}

		try {




			Tuple t = new Tuple(value);
			// System.out.println(t);

			// replace value with pointer when optimization is on
			if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
				value.set(pathids.getTupleID(context, key.get())); // key indicates offset in TextInputFormat
				// TODO # symbol in settings
			}


			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guardid
					out1.set(""+guardID);
					context.write(value, out1);
					context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
					context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(Integer.SIZE/8 + value.getLength());
					
					// output tuple value
					// only when pointer optimization is on
					// to be able to recover the tuple in the reducer
					if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
						out2.set("#"+t.toString());
						context.write(value, out2);
						if (print){
							LOG.error("Mapper 2 output: " + value.toString() + ":" + out2.toString());
						}
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(out2.getLength() + value.getLength());
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
