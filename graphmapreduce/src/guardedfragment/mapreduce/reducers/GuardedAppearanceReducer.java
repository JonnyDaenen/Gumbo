package guardedfragment.mapreduce.reducers;

import guardedfragment.mapreduce.mappers.GuardedMapper;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFSerializer;

import java.util.Set;
import java.io.IOException;

import mapreduce.data.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Phase 1 Reducer
 * 
 * Input: Si(a,b) : set of tuples 
 * Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R (guarded relation is determined from the
 * key)
 * 
 * This reducer checks for each data tuple Si(a,b) whether: - it appears in R -
 * it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 * 
 */
public class GuardedAppearanceReducer extends Reducer<Text, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(GuardedAppearanceReducer.class);
	
	// RelationSchema guardSchema;
	GFAtomicExpression guard;
	Set<GFAtomicExpression> guarded;

	public GuardedAppearanceReducer() {

	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		GFSerializer serializer = new GFSerializer();

		// load guard
		try {
			String guardString = conf.get("guard");
			this.guard = serializer.deserializeGuard(guardString);
			// LOG.error(guard);
		} catch (Exception e) {
			throw new InterruptedException("No guard information supplied");
		}

		// load guarded
		try {
			String guardString = conf.get("guarded");
			this.guarded = serializer.deserializeGuarded(guardString);
			// LOG.error(guardedRelations);
		} catch (Exception e) {
			throw new InterruptedException("No guarded information supplied");
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		boolean foundKey = false;
		String stringKey = key.toString();

		LOG.error("KEY: " + key.toString());
		
		// look if data is present in the guarded relation
		for (Text value : values) {
			if (stringKey.equals(value.toString())) {
				LOG.error("Key found:" + value.toString());
				foundKey = true;
				break;
			}
		}

		// if it is
		if (foundKey) {
			Tuple tKey = new Tuple(stringKey);

			// check the tuples that match the guard
			for (Text value : values) {
				LOG.error("inspecting value:" + value.toString());
				Tuple t = new Tuple(value.toString());
				
				if (guard.matches(t)) {
					
					// TODO comment, this works because of guarding
					for (GFAtomicExpression gf : guarded) {
						if (gf.matches(tKey)) {
							LOG.error("inspecting value:" + value.toString());
							context.write(null, new Text(t.generateString() + ";" + gf.generateString()));
						}
					}
				}
			}
		}

	}

	/*
	 * protected void oldreduce(Text key, Iterable<Text> values, Context
	 * context) throws IOException, InterruptedException {
	 * 
	 * boolean guardFound = false; boolean guardedFound = false;
	 * 
	 * Tuple guardTuple = null; Tuple guardedTuple;
	 * 
	 * // determine guarded tuple and schema guardedTuple = new
	 * Tuple(key.toString()); RelationSchema guardedSchema =
	 * guardedTuple.extractSchema();
	 * 
	 * // checkfor guard and guarded for (Text value : values) { Tuple t = new
	 * Tuple(value.toString());
	 * 
	 * // check if it is the guarded schema if
	 * (t.satisfiesSchema(guardedSchema)) guardedFound = true;
	 * 
	 * // check if it is the guard schema (if so, keep track of it) if
	 * (t.satisfiesSchema(guardSchema)) { guardFound = true; guardTuple = t; }
	 * 
	 * // stop when both are found if (guardFound && guardedFound) break; }
	 * 
	 * // write output if both are found if ( guardFound && guardedFound )
	 * context.write(null, new
	 * Text(guardedTuple.generateString()+";"+guardTuple.generateString()));
	 * 
	 * }
	 */

}
