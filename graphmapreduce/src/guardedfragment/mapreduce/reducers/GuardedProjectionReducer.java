package guardedfragment.mapreduce.reducers;

import guardedfragment.data.Tuple;
import guardedfragment.data.RelationSchema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Phase 2 Reducer
 * 
 * Input: Si(a,b) : set of tuples
 * Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R, Guarded relations Si
 * 
 * This reducer checks for each data tuple Si(a,b) whether:
 * - it appears in R
 * - it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedProjectionReducer extends Reducer<Text, Text, Text, Text>{
	
	Set<RelationSchema> relations;
	RelationSchema outputrelation;
	
	public GuardedProjectionReducer() {
		// TODO
	}
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Set<RelationSchema> foundRelations = new HashSet<RelationSchema>();
		Set<Tuple> outputTuples = new HashSet<Tuple>();
		
		boolean allRelationsFound = false;
		
		// Record presence of all required relations
		for (Text value : values) {
			Tuple t = new Tuple(value.toString());
			
			if (t.satisfiesSchema(outputrelation))
				outputTuples.add(t);
			
			RelationSchema s = t.extractSchema();
			
			if(relations.contains(s))
				foundRelations.add(s);
			
		}
		
		// if all relations are found 
		if(foundRelations.size() == relations.size())
			for (Tuple t : outputTuples) {
				String value = t.generateString();
				context.write(key, new Text(value));
			}
		
	}
	
	

}
