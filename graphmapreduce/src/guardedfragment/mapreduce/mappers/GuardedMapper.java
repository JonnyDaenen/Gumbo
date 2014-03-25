package guardedfragment.mapreduce.mappers;

import java.io.IOException;
import java.util.Map;

import mapreduce.data.Projection;
import mapreduce.data.RelationSchema;
import mapreduce.data.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
 * Phase: Basic Guarded - Phase 1 Mapper
 * 
 * Input: set of relations Si and one guarded relation R
 * Output: key:val where key is each value in Si, and the value is either the value of Si, or the value of R
 * 
 * Configuration: Guarding relation R, guarded relations Si, mapping between R and Si
 * 
 * For each Si, each value is always output. For a tuple R(a',b'), Si(a,b) : R(a',b') is output when the following holds:
 * - R(x',y') is the guard
 * - Si(x,y) appears in the boolean combination
 * - x subset x' and y subset y'
 * - each value in {a,b} appears on the "correct" positions in R(a',b') (i.e. according to the equality type)
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Map<RelationSchema, Projection> projections;
	boolean passNonMatches;
	
	
	public GuardedMapper(Map<RelationSchema, Projection> projections, boolean passNonMatches) {
		super();
		this.projections = projections;
		this.passNonMatches = passNonMatches;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// convert value to tuple
		Tuple t = new Tuple(value.toString());
		
		RelationSchema s = t.extractSchema();
		
		// if there is a mapping present, we map it
		if (projections.containsKey(s)) {
			
			// get projection
			Projection projection = projections.get(s);
			
			// extract relation from tuple
			Tuple newTuple = projection.project(t);
			
			// lookup projection associated with relation
			String newkey = newTuple.generateString();
			String newvalue = t.generateString();
			context.write(new Text(newkey),new Text(newvalue));
		}
		// if not we just pass the tuple, if enabled
		else if (passNonMatches){
			
			// we don't use original values because it may be in the wrong format...
			String newvalue = t.generateString();
			context.write(new Text(newvalue),new Text(newvalue));
			
		}
		
		
		
		
	}

}
