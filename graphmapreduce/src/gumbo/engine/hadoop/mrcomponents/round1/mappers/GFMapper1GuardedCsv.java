/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.general.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.hadoop.mrcomponents.tools.RelationResolver;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Also outputs the atoms when a guarded atom is projected onto them.
 * Only allows tuples from 1 relation as input.
 * 
 * @author Jonny Daenen
 * 
 */

public class GFMapper1GuardedCsv extends GFMapper1GuardedRelOptimized {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedCsv.class);
	private RelationResolver resolver;
	private StringBuilder stringBuilder;
	private Map1GuardedAlgorithm algo;
	private Text buffer;
	private byte[] open;
	private byte[] close;
	private String namebytes;
	private Tuple tup;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);

		try {
			resolver = new RelationResolver(eso);
			// pre-cache
			resolver.extractRelationSchema(context);

			Map1GuardedMessageFactoryInterface msgFactory = new Map1GuardedMessageFactory(context,settings,eso);
			algo = new Map1GuardedAlgorithm(eso, msgFactory,settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn));

			buffer = new Text();

			open = "(".getBytes();
			close = ")".getBytes();
			

			tup = new Tuple("_",new String[0]);

			// find out relation name
			RelationSchema rs = resolver.extractRelationSchema(context);
			namebytes = rs.getName();

		} catch (Exception e) {
			throw new InterruptedException(e.getMessage());
		}

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


			//			super.map(key, value, context);
			tup.initialize(namebytes, value.getBytes(), value.getLength());
			algo.run(tup, key.get());

		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		}



	}


}
