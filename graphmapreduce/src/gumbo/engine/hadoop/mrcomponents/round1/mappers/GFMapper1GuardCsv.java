/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.general.algorithms.Map1GuardAlgorithm;
import gumbo.engine.general.factories.Map1GuardMessageFactoryInterface;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
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
 * Outputs the atoms when a guard is projected onto them.
 * The input is in csv format, the output is in relational format.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardCsv extends GFMapper1GuardRelOptimized {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardCsv.class);


	RelationResolver resolver;

	private Text buffer;
	private byte [] open;
	private byte [] close;

	private Map1GuardAlgorithm algo;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);

		try {
			resolver = new RelationResolver(eso);
			// pre-cache
			resolver.extractRelationSchema(context);

			Map1GuardMessageFactoryInterface msgFactory = new Map1GuardMessageFactory(context,settings,eso);
			algo = new Map1GuardAlgorithm(eso, msgFactory,settings);

			buffer = new Text();

			open = "(".getBytes();
			close = ")".getBytes();
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

			// find out relation name
			RelationSchema rs = resolver.extractRelationSchema(context);
			byte [] namebytes = rs.getName().getBytes();


			// wrap tuple in relation name
			buffer.clear();
			buffer.append(namebytes,0,namebytes.length);
			buffer.append(open,0,open.length);
			buffer.append(value.getBytes(),0,value.getLength());
			buffer.append(close,0,close.length);

			Tuple t = new Tuple(buffer.getBytes(),buffer.getLength());
			algo.run(t, key.get());


		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		}

	}

}
