package guardedfragment.mapreduce.reducers;

import java.io.IOException;

import mapreduce.data.RelationSchema;
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
	
	RelationSchema guardSchema;
	

	public GuardedAppearanceReducer(RelationSchema guardSchema) {
		this.guardSchema = guardSchema;
	}
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
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
			// FIXME what with multiple guard tuples? -> output all?
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
	
	

}
