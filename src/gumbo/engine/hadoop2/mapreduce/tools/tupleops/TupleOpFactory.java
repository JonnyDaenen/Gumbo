package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

/**
 * Collection of factory methods to construct tuple operations to be used in mappers and reducers.
 * 
 * @author Jonny Daenen
 *
 */
public class TupleOpFactory {
	

	private static final Log LOG = LogFactory.getLog(TupleOpFactory.class);

	public static TupleProjection[] createMap1Projections(String relation, long fileid,
			Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap, boolean merge) {


		List<TupleProjection> projections = getGuardProjections(relation, fileid, queries, atomidmap);
		projections.addAll(getGuardedProjections(relation, queries, atomidmap));

		LOG.info("Projections before merge:");
		LOG.info(projections);
		// is requested, merge projections where possible
		if (merge)
			projections = mergeProjections(projections);
		LOG.info("Projections after merge:");
		LOG.info(projections);
		
		// OPTIMIZE projection dependencies

		return projections.toArray(new TupleProjection[0]);
	}

	private static List<TupleProjection> mergeProjections(List<TupleProjection> projections) {

		List<TupleProjection> mergedProjections = new ArrayList<>(projections.size());
		List<TupleProjection> todo = new ArrayList<>(projections);


		while (!todo.isEmpty()) {

			List<TupleProjection> remainder = new ArrayList<>(todo.size());
			// get first
			TupleProjection pi1 = todo.get(0);

			for (int i = 1; i < todo.size(); i++) {
				TupleProjection pi2 = todo.get(i);

				if (pi1.canMerge(pi2) && pi2.canMerge(pi1)) {
					pi1 = pi1.merge(pi2);
				} else {
					remainder.add(pi2);
				}
			}

			mergedProjections.add(pi1);
			
			todo = remainder;
		}

		return mergedProjections;
	}

	private static List<TupleProjection> getGuardProjections(String relation, long fileid,
			Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap) {
		List<TupleProjection> projections = new ArrayList<>();

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
		return projections;
	}

	private static List<TupleProjection> getGuardedProjections(String relation, Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap) {

		List<TupleProjection> projections = new ArrayList<>();
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

		return projections;
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

	public static TupleProjection[] createMapMSJProjections(String relation, long fileid,
			Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap, boolean merge) {

		List<TupleProjection> projections = getGuardedProjections(relation, queries, atomidmap);
		projections.addAll(getGuardDataProjections(relation, queries, atomidmap));

		LOG.info("Projections before merge:");
		LOG.info(projections);
		// is requested, merge projections where possible
		if (merge)
			projections = mergeProjections(projections);
		LOG.info("Projections after merge:");
		LOG.info(projections);
		
		return projections.toArray(new TupleProjection[0]);
	}

	/**
	 * Also, when this relation is a guard, it creates data projections.
	 * Also, when this relation is guarded, it creates the guarded projections.
	 * For the guard, we assume each guarded atom has the same key.
	 * 
	 * @param relation relation name
	 * @param queries set of queries
	 * @param atomidmap atomstring-atomid mapping
	 * @param merge 
	 * 
	 * @return projection array
	 */
	private static Collection<? extends TupleProjection> getGuardDataProjections(String relation,
			Set<GFExistentialExpression> queries, Map<String, Integer> atomidmap) {

		List<TupleProjection> projections = new ArrayList<>();

		// for each guard that matches
		for (GFExistentialExpression query : queries) {
			GFAtomicExpression guard = query.getGuard();
			if (guard.getName().equals(relation)) {

				// key of all atoms is the same, so we just need one!
				GFAtomicExpression guarded = query.getGuardedAtoms().iterator().next();

				// create projection
				GuardDataProjection pi = new GuardDataProjection(relation, guard, guarded, query.getId()); 
				projections.add(pi);


			}
		}
		
		return projections;
	}

}
