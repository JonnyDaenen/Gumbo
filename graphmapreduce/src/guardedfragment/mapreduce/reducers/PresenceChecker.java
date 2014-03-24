package guardedfragment.mapreduce.reducers;

import guardedfragment.data.RelationSchema;
import guardedfragment.structure.MyTuple;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer that checks the set of each key for the presence of certain relations.
 * When all relations are found, the key is output together with one of the values in the set.
 * This value is indicated by a relation. 
 * TODO what if there are more the same tuples?
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class PresenceChecker extends Reducer<Text, Text, Text, Text>{
	
	Set<RelationSchema> relations;
	RelationSchema outputrelation;
	
	public PresenceChecker() {
		// TODO
	}
	

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Set<RelationSchema> foundRelations = new HashSet<RelationSchema>();
		Set<MyTuple> outputTuples = new HashSet<MyTuple>();
		
		boolean allRelationsFound = false;
		
		// Record presence of all required relations
		for (Text value : values) {
			MyTuple t = new MyTuple(value.toString());
			
			if (t.satisfiesSchema(outputrelation))
				outputTuples.add(t);
			
			RelationSchema s = t.extractSchema();
			
			if(relations.contains(s))
				foundRelations.add(s);
			
		}
		
		// if all relations are found 
		if(foundRelations.size() == relations.size())
			for (MyTuple t : outputTuples) {
				String value = t.generateString();
				context.write(key, new Text(value));
			}
		
	}
	
	

}
