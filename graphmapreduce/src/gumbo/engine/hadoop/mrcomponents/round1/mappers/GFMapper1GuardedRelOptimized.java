/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.structures.data.Tuple;

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

	private Map1GuardedAlgorithm algo;

	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Map1GuardedMessageFactory msgFactory = new Map1GuardedMessageFactory(context,settings,eso);
		algo = new Map1GuardedAlgorithm(eso, msgFactory);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		try {
			
			Tuple t = new Tuple(value.getBytes(),value.getLength());
			algo.run(t, key.get());
			
		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}


}
