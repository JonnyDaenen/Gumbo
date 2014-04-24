package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.DeserializeException;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import mapreduce.data.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Round 1 Reducer
 * 
 * Input: Si(a,b) : set of tuples Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R (guarded relation is determined from the
 * key)
 * 
 * This reducer checks for each data tuple Si(a,b) whether: - it appears in R -
 * it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 * 
 */
public class GuardedAppearanceReducer extends Reducer<Text, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(GuardedAppearanceReducer.class);

	// RelationSchema guardSchema;
	Set<GFExistentialExpression> formulaSet;

	public GuardedAppearanceReducer() {

	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
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
	protected void reduce(Text key, Iterable<Text> tvalues, Context context) throws IOException, InterruptedException {

		String stringKey = key.toString();
		Tuple keyTuple = new Tuple(stringKey);

		// convert value set to tuple set
		Set<Tuple> tuples = new HashSet<Tuple>();

		for (Text t : tvalues) {
			Tuple ntuple = new Tuple(t.toString());
			tuples.add(ntuple);
		}
		

		// OPTIMIZE can we push for-loop inside?
		for (GFExistentialExpression formula : formulaSet) {

			boolean foundKey = false;
			GFAtomicExpression guard = formula.getGuard();

			// If the guard matches the key, we output the key itself and then
			// stop processing this formula
			// This is because other tuples cannot appear as a value; they need
			// to ...
			// TODO this is wrong I think:
			// consider the following: Ey G(x,y) and G(x,x)
			// with (1,2) (1,1)
			// this would not output anything for the guarded part
//			if (guard.matches(keyTuple)) {
//				// LOG.error("the guard tuple " + tKey.toString());
//				context.write(null, new Text(keyTuple.generateString() + ";"));
//				continue; // go to next formula
//			}

			// look if data (key) is present in a guarded relation (one of the
			// values)
			for (Tuple tuple : tuples) {
				if (keyTuple.equals(tuple)) {
					foundKey = true;
					break;
				}
			}


			// if the guarded tuple is actually in the database
			if (foundKey) {

				// find tuples...
				for (Tuple tuple : tuples) {

					// ...that match the guard
					if (guard.matches(tuple)) {

						Tuple guardTuple = tuple;

						// get all atomics in the formula
						Set<GFAtomicExpression> guarded = formula.getChild().getAtomic();

						// for each atomic
						for (GFAtomicExpression guardedAtom : guarded) {

							// project the guard tuple onto current atom
							GFAtomProjection p = new GFAtomProjection(guard, guardedAtom);
							Tuple projectedTuple;
							try {
								projectedTuple = p.project(guardTuple); // TODO
																		// is
																		// this
																		// MRT?

								// check link between guard variables and atom
								// variables
								// TODO explain
								if (projectedTuple.equals(keyTuple)) {
									context.write(null,
											new Text(guardTuple.generateString() + ";" + guardedAtom.generateString()));
								}

							} catch (NonMatchingTupleException e) {
								// should not happen
								e.printStackTrace();
							}

						}
					}
				}

			}

			// when only guards appear in the value set, we need to keep those
			// alive (Si's are all FALSE).
			// OPTIMIZE This is not necessary when others have been output
			for (Tuple tuple : tuples) {
				if (guard.matches(tuple)) {
					context.write(null, new Text(tuple.generateString() + ";"));
				}
			}

		}

	}

}
