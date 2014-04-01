package guardedfragment.mapreduce.mappers;

import guardedfragment.mapreduce.GFMRlevel1Example;
import guardedfragment.structure.DeserializeException;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFSerializer;
import guardedfragment.structure.GuardedProjection;
import guardedfragment.structure.NonMatchingTupleException;

import java.io.IOException;
import java.util.Set;

import mapreduce.data.RelationSchema;
import mapreduce.data.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

	private static final Log LOG = LogFactory.getLog(GuardedMapper.class);

	GFAtomicExpression guard;
	Set<GFAtomicExpression> guardedRelations;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

		super.setup(context);
		Configuration conf = context.getConfiguration();
		GFSerializer serializer = new GFSerializer();

		// load guard
		try {
			String guardString = conf.get("guard");
			this.guard = serializer.deserializeGuard(guardString);
//			LOG.error(guard);
		} catch (Exception e) {
			throw new InterruptedException("No guard information supplied");
		}

		// load guarded
		try {
			String guardString = conf.get("guarded");
			this.guardedRelations = serializer.deserializeGuarded(guardString);
//			LOG.error(guardedRelations);
		} catch (Exception e) {
			throw new InterruptedException("No guarded information supplied");
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// convert value to tuple
		Tuple t = new Tuple(value.toString());

		// check if tuple matches guard
		if (guard.matches(t)) {
//			LOG.error(t + " matches " + guard);
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// get projection
				// OPTIMIZE do this when initializing
				GuardedProjection gp = new GuardedProjection(guard, guarded);
				Tuple tprime;
				try {
					// project to key
					tprime = gp.project(t);
				
					if (guarded.matches(tprime)) {

						LOG.error(tprime + " matches' " + guarded + " val: " + t);
						// project to key and write out
						context.write(new Text(tprime.toString()), new Text(t.toString()));

					}
				} catch (NonMatchingTupleException e1) {
					// should not happen!
					e1.printStackTrace();
				}

			}

		}
		// if schema is not same as guard
		else {
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// if so, output tuple with same key and value
				if (guarded.matches(t)) {
//					LOG.error(t + " matches " + guarded);
					context.write(new Text(t.toString()), new Text(t.toString()));
					break;
				}
			}
		}
	}
}
