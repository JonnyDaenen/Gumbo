/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A Hadoop-wrapper aroung GFMapper. When it is initialized by Hadoop, 
 * an instance of a specified GFMapper is created. This is done by passing the
 * class name through the <code>GFMapperClass</code> option of the configuration.
 * The set of formulas is to be specified using the <code>formulaset</code> option.
 * This set must be presented as a serialized set of GFExpressions, created using a
 * {@link GFPrefixSerializer}.
 * 
 * @author Jonny Daenen
 *
 */
public class GFMapperHadoop extends Mapper<LongWritable, Text, Text, Text> {
	

	private static final Log LOG = LogFactory.getLog(GFMapperHadoop.class);
	
	Set<GFExistentialExpression> formulaSet;
	GFMapper mapper;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();
		
		// load mapper
		try {
			LOG.debug("Instantiating Mapper");
			mapper = GFMapper.class.getClassLoader().loadClass(conf.get("GFMapperClass")).asSubclass(GFMapper.class).newInstance();
		} catch (Exception e1) {
			e1.printStackTrace();
			throw new InterruptedException("Mapper initialisation error: " + e1.getMessage());
		}

		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}

		} catch (DeserializeException e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}

	}
	
	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		Set<Pair<String, String>> result = mapper.map(value.toString(), formulaSet);
		
		for (Pair<String, String> pair : result) {
			String k = pair.fst;
			String val = pair.snd;
			context.write(new Text(k), new Text(val));
		}
		
		
	}
	
	

}
