/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;

import mapreduce.guardedfragment.executor.hadoop.ExecutorSettings;
import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Also outputs the atoms when a guarded atom is projected onto them.
 * Accepts tuples from multiple relations, in rel-format.
 * 
 * @author Jonny Daenen
 * 
 */

public class GFMapper1GuardedRel extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedRel.class);
	Text proofSymbol;
	
	/**
	 * @see mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		super.setup(context);
		proofSymbol = new Text(settings.getProperty(settings.PROOF_SYMBOL));
		
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		boolean print = false;
//		if (value.toString().contains("1000")) {
//			LOG.error("Mapper1: " + value);
//			print = true;
//		}

		// trim is necessary to remove extra whitespace
		value.set(value.toString().trim());
		Tuple t = new Tuple(value.toString());
		// System.out.println(t);

		// guarded existance output
		for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

			// if no guarded expression matches this tuple, it will not be output
			if (guarded.matches(t)) {
				// reduce data size by using a constant symbol
				if (settings.getBooleanProperty(ExecutorSettings.guardedIdOptimizationOn)) {
					context.write(value, proofSymbol);
					context.getCounter(GumboMap1Counter.PROOF_OF_EXISTENCE).increment(1);
					context.getCounter(GumboMap1Counter.PROOF_OF_EXISTENCE_BYTES).increment(value.getLength()+1);
				} else {
					context.write(value, value);
					context.getCounter(GumboMap1Counter.PROOF_OF_EXISTENCE).increment(1);
					context.getCounter(GumboMap1Counter.PROOF_OF_EXISTENCE_BYTES).increment(value.getLength()*2);
				}
				
//				if (print) {
//					LOG.error("Mapper1 output: " + value + " " + value);
//				}
				//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
				break;
			}
		}

	}


}
