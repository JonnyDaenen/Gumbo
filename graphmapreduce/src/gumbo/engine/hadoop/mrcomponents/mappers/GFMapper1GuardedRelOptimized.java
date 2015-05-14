/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
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

public class GFMapper1GuardedRelOptimized extends GFMapper1Identity {

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedRelOptimized.class);
	private Map1GuardedMessageFactory msgFactory;

	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		msgFactory = new Map1GuardedMessageFactory(context,settings,eso);		
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



		// CLEAN remove spaces? -> trim inside Tuple class?
		Tuple t = new Tuple(value.toString());

		msgFactory.loadGuardedValue(t);

		// OPTIMIZE search for guarded atom based on name
		// guarded existance output
		for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

			// if no guarded expression matches this tuple, it will not be output
			if (guarded.matches(t)) {
				msgFactory.sendAssert();

				// one assert message suffices
				break;
			}
		}

	}


}
