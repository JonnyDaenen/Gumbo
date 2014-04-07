package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFSerializer;
import guardedfragment.structure.GuardedProjection;
import guardedfragment.structure.MyGFParser;
import guardedfragment.structure.NonMatchingTupleException;

import java.io.IOException;
import java.util.Set;
import java.util.ArrayList;

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
	GFExistentialExpression formula;


	public GuardedAppearanceReducer() {

	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();

		// load guard
		try {
			String formulaString = conf.get("formula");
			MyGFParser parser = new MyGFParser(formulaString);
			formula = (GFExistentialExpression) parser.deserialize();
		} catch (Exception e) {
			throw new InterruptedException("No guarded information supplied");
		}
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> tvalues, Context context) throws IOException, InterruptedException {
	
		boolean foundKey = false;
		String stringKey = key.toString();
		Tuple tKey = new Tuple(stringKey);
		//Text w;
		
		ArrayList<String> ttvalues = new ArrayList<String>();
		LOG.error("The reducer for the key "+stringKey);
		for (Text v : tvalues) {
			LOG.error(v.toString());
			ttvalues.add(v.toString());
		}
		LOG.error("The values in the ArrayList: "+ttvalues.toString());
		LOG.error("=============================");

		String[] values = ttvalues.toArray(new String[ttvalues.size()]);

		GFAtomicExpression guard = formula.getGuard();
			
		if (guard.matches(tKey)) {
			//LOG.error("the guard tuple " + tKey.toString());
			context.write(null, new Text(tKey.generateString() + ";"));
			return;
		}
		
		// look if data is present in the guarded relation
		for (int i=0;i<values.length;i++) {
			//LOG.error("Inside the loop");
			if (stringKey.equals(values[i])) {
				//LOG.error("Key found:" + values[i]);
				foundKey = true;
				break;
			}
		}

		Tuple t;
		GuardedProjection p;
		Set<GFAtomicExpression> guarded;

		if (foundKey) {
			// check the tuples that match the guard
			//LOG.error("Inside the if after the foundKey");
			for (int i=0;i<values.length;i++) {
				//LOG.error("Inside for loop");
				//LOG.error("inspecting value:" + values[i]);
				t = new Tuple(values[i]);
			
				if (guard.matches(t)) {
					
					guarded = formula.getChild().getAtomic();
					
					// TODO comment, this works because of guarding
					for (GFAtomicExpression gf : guarded) {
						p = new GuardedProjection(guard,gf);				
						try {
							if (p.project(t).equal(tKey)) {
								//LOG.error("inspecting value:" + values[i]);
								context.write(null, new Text(t.generateString() + ";" + gf.generateString()));
							}
						} catch (NonMatchingTupleException e) {
							e.printStackTrace();
						}
					}
				}
			}
		} else {
			
			for (int i=0;i<values.length;i++) {
				//LOG.error("inspecting value:" + values[i]);
				t = new Tuple(values[i]);
				
				if (guard.matches(t)) {
					//LOG.error("inspecting value:" + values[i]);
					context.write(null, new Text(t.generateString() + ";"));
						
				}
			}			
			
			
		}

	}

}
