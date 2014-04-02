package guardedfragment.mapreduce.mappers;

import guardedfragment.mapreduce.reducers.GuardedAppearanceReducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


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
	
	private static final Log LOG = LogFactory.getLog(GuardedAppearanceReducer.class);
	
	public GuardedBooleanMapper() {
		super();
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String stringValue = value.toString();
		LOG.error(stringValue);
		if (stringValue.contains(";")) {
			
			String[] t = stringValue.split(new String(";"));
			if (t.length == 2) {
				// key is the guard, value is the guarded tuple
				LOG.error("INSIDE THE MAPPER " + t[0] + " and " + t[1]);
				context.write(new Text(t[0]), new Text(t[1]));				
			} else {
				LOG.error("INSIDE THE MAPPER " + t[0] + " END");
				context.write(new Text(t[0]), new Text(new String()));
			}
		}
		

		
		
	}

}
