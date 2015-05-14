/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.io.IOException;

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

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedRel.class);
	Text proofSymbol;
	Text output;
	
	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		proofSymbol = new Text(settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL));
		output = new Text();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



		// CLEAN what with the spaces? -> trim inside?
		Tuple t = new Tuple(value.toString());
		
		
		if (settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn)) {
			value.set(value.toString()+proofSymbol);
		} 

		// OPTIMIZE search for guarded atom based on name
		// guarded existance output
		for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

			// if no guarded expression matches this tuple, it will not be output
			if (guarded.matches(t)) {
				// reduce data size by using a constant symbol
				if (settings.getBooleanProperty(HadoopExecutorSettings.guardedIdOptimizationOn)) {
					context.write(value, proofSymbol);
					context.getCounter(GumboMap1Counter.ASSERT).increment(1);
					context.getCounter(GumboMap1Counter.ASSERT_BYTES).increment(value.getLength()+1);
				} else {
					output.set(t.toString());
					context.write(value, output);
					context.getCounter(GumboMap1Counter.ASSERT).increment(1);
					context.getCounter(GumboMap1Counter.ASSERT_BYTES).increment(value.getLength()*2);
				}
				
				// one assert message suffices
				break;
			}
		}

	}


}
