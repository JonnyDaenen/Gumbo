/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.reducers;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.planner.structures.operations.GFReducer;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

/**
 * @author Jonny Daenen
 * 
 */
public class GFReducer1Generic extends GFReducer implements Serializable {

	private static final long serialVersionUID = 1L;
	private final static String FILENAME = "tmp_round1_red.txt";

	/**
	 * @throws GFOperationInitException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFReducer#reduce(java.lang.String,
	 *      java.lang.Iterable)
	 */
	@Override
	// / OPTIMIZE iterable string?
	public Iterable<Pair<String, String>> reduce(String key, Iterable<? extends Object> values)
			throws GFOperationInitException {

		HashSet<Pair<String, String>> result = new HashSet<Pair<String, String>>();

		String stringKey = key;// .toString();
		Tuple keyTuple = new Tuple(stringKey);

		// convert value set to tuple set
		Set<Tuple> tuples = new HashSet<Tuple>();

		boolean foundKey = false;
		// look if data (key) is present in a guarded relation (one of the
		// values)
		for (Object t : values) {
			Tuple ntuple = new Tuple(t.toString());
			tuples.add(ntuple);

			if (keyTuple.equals(ntuple)) {
				foundKey = true;
				break;
			}
		}

		// OPTIMIZE can we push for-loop inside?
		for (GFExistentialExpression formula : expressionSet) {

			GFAtomicExpression guard = formula.getGuard();
			// get all atomics in the formula
			Collection<GFAtomicExpression> guarded = getGuardeds(formula);

			// if the guarded tuple is actually in the database
			if (foundKey) {

				// find tuples...
				for (Tuple tuple : tuples) {

					// ...that match the current guard
					if (guard.matches(tuple)) {

						Tuple guardTuple = tuple;
						String guardTupleString = null;

						// for each atomic
						for (GFAtomicExpression guardedAtom : guarded) {

							// project the guard tuple onto current atom
							GFAtomProjection p = new GFAtomProjection(guard, guardedAtom);
							Tuple projectedTuple;
							try {
								projectedTuple = p.project(guardTuple);

								// check link between guard variables and atom
								// variables
								// It is possible that a guard is linked to two
								// atoms:
								// e.g. R(x,y) ^ S(x) ^ S(y)
								// that is why we need to check which one should
								// be output
								// e.g. S(x) vs. S(y)
								if (projectedTuple.equals(keyTuple)) {
									// OLD: context.write(null,new
									// Text(guardTuple.generateString() + ";" +
									// guardedAtom.generateString()));
									if(guardTupleString == null)
										guardTupleString = guardTuple.generateString() + ";";
									result.add(new Pair<String, String>(
											guardTupleString + guardedAtom.generateString(), FILENAME));
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
					// context.write(null, new Text(tuple.generateString() +
					// ";"));
					result.add(new Pair<String, String>(tuple.generateString() + ";", FILENAME));
				}
			}

		}

		return result;
	}

}
