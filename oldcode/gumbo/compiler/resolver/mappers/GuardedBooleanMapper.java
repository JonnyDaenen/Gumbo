package gumbo.compiler.resolver.mappers;

import gumbo.compiler.resolver.reducers.GuardedAppearanceReducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/** 
 * Phase: Basic Guarded - Round 2 Mapper
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
	

	
	private static final Log LOG = LogFactory.getLog(GuardedAppearanceReducer.class);
	
	public GuardedBooleanMapper() {
		super();
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String stringValue = value.toString();
		
		if (stringValue.contains(";")) {
			
			String[] t = stringValue.split(new String(";"));
			if (t.length == 2) { // guarded atoms that are true
				
				// key is the guard, value is the guarded tuple
				context.write(new Text(t[0]), new Text(t[1]));				
			
			// propagate keep alive
			} else { 
				context.write(new Text(t[0]), new Text(new String()));
			}
		}
		

		
		
	}

}
