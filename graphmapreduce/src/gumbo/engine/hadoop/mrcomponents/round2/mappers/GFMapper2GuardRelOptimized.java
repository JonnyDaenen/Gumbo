/**
 * Created: 16 May 2015
 */
package gumbo.engine.hadoop.mrcomponents.round2.mappers;

import gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity;
import gumbo.engine.hadoop.mrcomponents.round2.algorithms.Map2GuardAlgorithm;
import gumbo.engine.hadoop.mrcomponents.round2.algorithms.Map2GuardMessageFactory;
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
public class GFMapper2GuardRelOptimized extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper2GuardRelOptimized.class);

	protected Map2GuardMessageFactory msgFactory;
	private Map2GuardAlgorithm algo;



	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		msgFactory = new Map2GuardMessageFactory(context,settings,eso);		
		algo = new Map2GuardAlgorithm(eso, msgFactory);
		
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
			Tuple t = new Tuple(value.getBytes(),value.getLength());
			algo.run(t, key.get());
	}




}
