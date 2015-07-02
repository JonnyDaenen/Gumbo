/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.structures.data.Tuple;

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
public class GFMapper1GuardRelOptimized extends GFMapper1Identity {



	private static final Log LOG = LogFactory.getLog(GFMapper1GuardRelOptimized.class);

	private Map1GuardAlgorithm algo;



	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Map1GuardMessageFactory msgFactory = new Map1GuardMessageFactory(context,settings,eso);		
		algo = new Map1GuardAlgorithm(eso, msgFactory,settings);

		// dummy 
		LOG.getClass();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try{
			Tuple t = new Tuple(value.getBytes(), value.getLength());
			algo.run(t, key.get());
		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}




}
