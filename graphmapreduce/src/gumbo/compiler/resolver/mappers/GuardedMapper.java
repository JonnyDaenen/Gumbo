package gumbo.compiler.resolver.mappers;

import gumbo.compiler.structures.data.Tuple;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.DeserializeException;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;
import gumbo.guardedfragment.gfexpressions.operations.GFAtomProjection;
import gumbo.guardedfragment.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Phase: Basic Guarded - Round 1 Mapper
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

	private static final Log LOG = LogFactory.getLog(GuardedMapper.class);

	Set<GFExistentialExpression> formulaSet;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		Configuration conf = context.getConfiguration();

		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}

		} catch (DeserializeException e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		for (GFExistentialExpression formula : formulaSet) {

			// convert value to tuple
			Tuple t = new Tuple(value.toString());
			LOG.trace("An original value: " + value.toString());

			GFAtomicExpression guard = formula.getGuard();
			Collection<GFAtomicExpression> guardedRelations = formula.getChild().getAtomic();

			// check if tuple matches guard
			if (guard.matches(t)) {

				// output guard:guard
				context.write(new Text(t.toString()), new Text(t.toString()));
				LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());

				for (GFAtomicExpression guarded : guardedRelations) {

					// get projection
					// OPTIMIZE do this when initializing
					GFAtomProjection gp = new GFAtomProjection(guard, guarded);
					Tuple tprime;
					try {
						// project to key
						tprime = gp.project(t);

						if (guarded.matches(tprime)) {

							// LOG.error(tprime + " matches' " + guarded +
							// " val: " + t);
							// project to key and write out
							context.write(new Text(tprime.toString()), new Text(t.toString()));
							LOG.trace("The first Mapper outputs the pair: " + tprime.toString() + " : " + t.toString());

						}
					} catch (NonMatchingTupleException e1) {
						// should not happen!
						e1.printStackTrace();
					}

				}

			}

			
			// ATTENTION: do not use ELSE here
			// the guard can appear as guarded too!
			
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// if so, output tuple with same key and value
				if (guarded.matches(t)) {
					// LOG.error(t + " matches " + guarded);
					context.write(new Text(t.toString()), new Text(t.toString()));
					LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());
					// break;
				}
			}

		}

	}
}
