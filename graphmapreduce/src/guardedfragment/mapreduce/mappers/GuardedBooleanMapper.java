package guardedfragment.mapreduce.mappers;

import guardedfragment.structure.GFAtomicExpression;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Phase: Basic Guarded - Phase 2 Mapper
 * 
 * Input: a sets of 2 tuples Si(a,b);R(a',b').
 * Output: R(a',b'):ti, where ti is the boolean variable corresponding to Si.
 * This essentially corresponds to 'true' for this boolean variable.
 * 
 * TODO not clear how to determine ti yet
 * 
 * Configuration: Guarding relation R, mapping Si->ti
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 *
 */
public class GuardedBooleanMapper extends Mapper<LongWritable, Text, Text, Text> {
	
//	GFAtomicExpression guard;
//	Set<GFAtomicExpression> guardedRelations;

//	public GuardedBooleanMapper(GFAtomicExpression guard, Set<GFAtomicExpression> guardedRelations) {
//		super();
//		this.guard = guard;
//		this.guardedRelations = guardedRelations;
//	}
//	
	
	public GuardedBooleanMapper() {
		super();
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String stringValue = value.toString();
		if (stringValue.contains(";")) {
			
			String[] t = stringValue.split(new String(";"));
			if (t.length == 2) {
				// key is the guard, value is the guarded tuple
				context.write(new Text(t[0]), new Text(t[1]));				
			}
		}
		

		
		
	}

}
