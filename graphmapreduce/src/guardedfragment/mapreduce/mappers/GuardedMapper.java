package guardedfragment.mapreduce.mappers;

import guardedfragment.structure.GFAtomicExpression;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
 * Configuration: 
 * - atomic Guarding expression R
 * - guarded relations Si, 
 * - mapping between R and Si
 * 
 * For each Si, each value is always output. For a tuple R(a',b'), Si(a,b) : R(a',b') is output when the following holds:
 * - R(x',y') is the guard
 * - TODO R(a',b') belongs to guard
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
	
	GFAtomicExpression guard;
	Set<GFAtomicExpression> guardedRelations;
	
	
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
		
		// get schema
		RelationSchema s = t.extractSchema();
		
		// if schema is same as guard
		// check if tuple matches guard
		// if so, output mapping to each Si
		
		// if schema is not same as guard
		// check if it matches an Si
		// if so, output tuple with same key and value
		
		
		
		
	}

}
