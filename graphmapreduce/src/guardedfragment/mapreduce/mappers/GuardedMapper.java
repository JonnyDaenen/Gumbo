package guardedfragment.mapreduce.mappers;

import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GuardedProjection;
import guardedfragment.structure.NonMatchingTupleException;

import java.io.IOException;
import java.util.Set;

import mapreduce.data.RelationSchema;
import mapreduce.data.Tuple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Phase: Basic Guarded - Phase 1 Mapper
 * 
 * Input: set of relations Si and one guarded relation R Output: key:val where
 * key is each value in Si, and the value is either the value of Si, or the
 * value of R
 * 
 * Configuration: - atomic Guarding expression R - guarded relations Si, -
 * mapping between R and Si
 * 
 * For each Si, each value is always output. For a tuple R(a',b'), Si(a,b) :
 * R(a',b') is output when the following holds: - R(x',y') is the guard - TODO
 * R(a',b') belongs to guard - Si(x,y) appears in the boolean combination - x
 * subset x' and y subset y' - each value in {a,b} appears on the "correct"
 * positions in R(a',b') (i.e. according to the equality type)
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 * 
 */
public class GuardedMapper extends Mapper<LongWritable, Text, Text, Text> {

	GFAtomicExpression guard;
	Set<GFAtomicExpression> guardedRelations;

	public GuardedMapper(GFAtomicExpression guard, Set<GFAtomicExpression> guardedRelations) {
		super();
		this.guard = guard;
		this.guardedRelations = guardedRelations;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// convert value to tuple
		Tuple t = new Tuple(value.toString());

		// get schema
		RelationSchema tupleSchema = t.extractSchema();
		// OPTIMIZE do this when initializing
		RelationSchema guardSchema = guard.extractRelationSchema();

		// if schema is same as guard
		if (tupleSchema.equals(guardSchema)) {

			// check if tuple matches guard
			if (guard.matches(t)) {

				// check if tuple matches a guarded atom
				for (GFAtomicExpression guarded : guardedRelations) {

					// if so, output mapping to each Si
					if (guarded.matches(t)) {
						// get projection
						// OPTIMIZE do this when initializing
						GuardedProjection gp = new GuardedProjection(guard, guarded);

						Tuple tprime;
						try {
							// project to key and write out
							tprime = gp.project(t);
							context.write(new Text(tprime.toString()), new Text(t.toString()));
						} catch (NonMatchingTupleException e) {
							// should not happen!
							e.printStackTrace();
						}

					}
				}

			}
		}
		// if schema is not same as guard
		else {
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// if so, output tuple with same key and value
				if (guarded.matches(t)) {
					context.write(new Text(t.toString()), new Text(t.toString()));
				}
			}
		}
	}
}
