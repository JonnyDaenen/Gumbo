/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.reducers;

import gumbo.compiler.resolver.operations.GFReducer;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Jonny Daenen
 * 
 */
public class GFReducerHadoop extends Reducer<Text, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(GFReducerHadoop.class);

	GFReducer reducer;

	Set<GFExistentialExpression> formulaSet;

	private MultipleOutputs<Text, Text> mos;

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs(context);

		Configuration conf = context.getConfiguration();

		// load reducer
		try {
			LOG.debug("Instantiating Reducer");
			reducer = this.getClass().getClassLoader().loadClass(conf.get("GFReducerClass"))
					.asSubclass(GFReducer.class).newInstance();
		} catch (Exception e1) {
			throw new InterruptedException("Reducer initialisation error: " + e1.getMessage());
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

			reducer.setExpressionSet(formulaSet);
		} catch (Exception e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}

	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		reducer.reduce(key, values, mos);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

}
