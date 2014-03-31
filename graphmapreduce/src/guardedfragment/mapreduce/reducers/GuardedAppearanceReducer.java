package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.GFAtomicExpression;
import java.util.Set;

import java.io.IOException;

import mapreduce.data.Tuple;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Phase 1 Reducer
 * 
 * Input: Si(a,b) : set of tuples
 * Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R (guarded relation is determined from the key)
 * 
 * This reducer checks for each data tuple Si(a,b) whether:
 * - it appears in R
 * - it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 *
 */
public class GuardedAppearanceReducer extends Reducer<Text, Text, Text, Text>{
	
	//RelationSchema guardSchema;
	GFAtomicExpression guard;
	Set<GFAtomicExpression> guarded;

	public GuardedAppearanceReducer(GFAtomicExpression g, Set<GFAtomicExpression> gf) {
		this.guard = g;
		this.guarded = gf;
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		boolean foundKey = false;
		String stringKey = key.toString();
		
		for (Text value : values) {
			if (stringKey.equals(value.toString())) {
				foundKey = true;
				break;
			}
		}
				
		if (foundKey) {	
			Tuple t;
			Tuple tKey = new Tuple(stringKey);
			
			for (Text value : values) {
				t = new Tuple(value.toString());
				if (guard.matches(t)) {
					for (GFAtomicExpression gf : guarded) {
						if (gf.matches(tKey)) {
							context.write(null, new Text(t.generateString()+";"+gf.generateString()));
						}
					}
				}				
			}
		}

	}

	

/*	protected void oldreduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		boolean guardFound = false;
		boolean guardedFound = false;		
		
		Tuple guardTuple = null;
		Tuple guardedTuple;
		
		// determine guarded tuple and schema
		guardedTuple = new Tuple(key.toString());
		RelationSchema guardedSchema = guardedTuple.extractSchema();
		
		// checkfor guard and guarded
		for (Text value : values) {
			Tuple t = new Tuple(value.toString());
			
			// check if it is the guarded schema
			if (t.satisfiesSchema(guardedSchema))
				guardedFound = true;
			
			// check if it is the guard schema (if so, keep track of it)
			if (t.satisfiesSchema(guardSchema)) {
				guardFound = true;
				guardTuple = t;
			}
			
			// stop when both are found
			if (guardFound && guardedFound)
				break;
		}
		
		// write output if both are found
		if ( guardFound && guardedFound )
			context.write(null, new Text(guardedTuple.generateString()+";"+guardTuple.generateString()));
		
	}
*/

}
