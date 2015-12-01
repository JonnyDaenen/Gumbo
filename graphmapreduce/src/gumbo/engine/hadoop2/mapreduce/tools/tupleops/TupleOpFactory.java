package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.mesos.Protos.Filters;

import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.conversion.GFtoBooleanConvertor;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

/**
 * Collection of factory methods to construct tuple operations to be used in mappers and reducers.
 * 
 * @author Jonny Daenen
 *
 */
public class TupleOpFactory {

	public static TupleProjection[] createMap1Projections(String relation, long fileid,
			Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap) {

		List<TupleProjection> projections = new ArrayList<>();


		// GUARD

		// for each guard that matches
		for (GFExistentialExpression query : queries) {
			GFAtomicExpression guard = query.getGuard();
			if (guard.getName().equals(relation)) {

				// for each guarded
				for (GFAtomicExpression guarded : query.getGuardedAtoms()) {
					// create projection
					GuardProjection pi = new GuardProjection(relation, fileid, guard, guarded, (byte) atomidmap.get(guarded.toString()).intValue()); 
					projections.add(pi);

				}
			}
		}

		// GUARDED
		
		for (GFExistentialExpression query : queries) {
			// for each guarded
			for (GFAtomicExpression guarded : query.getGuardedAtoms()) {
				if (guarded.getName().equals(relation)) {
					// create projection
					GuardedProjection pi = new GuardedProjection(relation, guarded, (byte) atomidmap.get(guarded.toString()).intValue()); 
					projections.add(pi);
				}

			}

		}


		// TODO merge where possible

		// OPTIMIZE projection dependencies

		return projections.toArray(new TupleProjection[0]);
	}

	/**
	 * Creates a set of tupleevaluators, one for each query.
	 * 
	 * @param queries set of active queries
	 * @param filemap mapping from output relations to file names
	 * @return an array of evaluators
	 */
	public static TupleEvaluator[] createRed2Projections(
			Set<GFExistentialExpression> queries, Map<String, 
			String> filemap, Map<String, Integer> atomidmap) {

		List<TupleEvaluator> projections = new ArrayList<>(queries.size());

		for (GFExistentialExpression query : queries) {
//			String filename = filemap.get(query.getOutputRelation().getName());
			TupleEvaluator te = new TupleEvaluator(query, query.getOutputRelation().getName(), atomidmap);
			projections.add(te);

		}

		return projections.toArray(new TupleEvaluator[0]);
	}


	/**
	 * Constructs a filter for the guard tuples that are (re-)read in the evaluation mapper.
	 * 
	 * @param atoms set of guard atoms
	 * @param relation the relation name
	 * @return a filter for guard tuples
	 */
	public static TupleFilter []  createMap2Filter(Set<GFAtomicExpression> atoms, String relation) {

		List<TupleFilter> filters = new ArrayList<>(atoms.size());

		for (GFAtomicExpression guard : atoms) {
			if (guard.getName().equals(relation)) {
				EqualityFilter eqt = new EqualityFilter(guard);
				filters.add(eqt);
			}
		}

		return filters.toArray(new TupleFilter[0]);
	}

}
