package guardedfragment.mapreduce.mappers;

import java.io.IOException;

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
 *
 */
public class GuardedBooleanMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// input has the form Si(a,b);R(a',b')
		// split input
		
		// convert to 2 tuples
		
		// get tuple that matches R
		
		// convert Si-tuple to boolean variable ti
		
		// output R(a',b'):ti
		
		
	}

}
