/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.reducers;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
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
public class GFReducer1Generic extends GFReducer {

	private final static String FILENAME = "tmp_round1_red.txt";

	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFReducer#reduce(java.lang.String,
	 *      java.lang.Iterable)
	 */
	@Override
	public Set<Pair<String, String>> reduce(String key, Iterable<? extends Object> values,
			Collection<GFExistentialExpression> expressionSet) {

		HashSet<Pair<String, String>> result = new HashSet<Pair<String, String>>();

		String stringKey = key.toString();
		Tuple keyTuple = new Tuple(stringKey);

		// convert value set to tuple set
		Set<Tuple> tuples = new HashSet<Tuple>();

		for (Object t : values) {
			Tuple ntuple = new Tuple(t.toString());
			tuples.add(ntuple);
		}

		// OPTIMIZE can we push for-loop inside?
		for (GFExistentialExpression formula : expressionSet) {

			boolean foundKey = false;
			GFAtomicExpression guard = formula.getGuard();

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
//									context.write(null,new Text(guardTuple.generateString() + ";" + guardedAtom.generateString()));
									result.add(new Pair<String, String>(guardTuple.generateString() + ";" + guardedAtom.generateString(), FILENAME));
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
			// TODO what if there is no S to contain a G, is there a G:G kv pair?
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
