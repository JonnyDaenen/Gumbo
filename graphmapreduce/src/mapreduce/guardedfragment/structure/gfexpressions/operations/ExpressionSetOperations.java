/**
 * Created: 25 Sep 2014
 */
package mapreduce.guardedfragment.structure.gfexpressions.operations;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.compiler.DirManager;
import mapreduce.guardedfragment.planner.structures.InputFormat;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConversionException;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConvertor;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 * 
 */
public class ExpressionSetOperations {

	private static final Log LOG = LogFactory.getLog(ExpressionSetOperations.class);

	protected Collection<GFExistentialExpression> expressionSet;

	protected HashMap<GFExistentialExpression, Collection<GFAtomicExpression>> guardeds;
	protected HashMap<GFExistentialExpression, BExpression> booleanChildExpressions;
	protected GFBooleanMapping booleanMapping;
	protected HashMap<GFExistentialExpression, GFAtomProjection> projections;

	protected HashMap<Pair<GFAtomicExpression, GFAtomicExpression>, GFAtomProjection> guardHasProjection;
	protected HashMap<GFAtomicExpression, Set<GFAtomicExpression>> guardHasGuard;

	protected Set<GFAtomicExpression> guardsAll;
	protected Set<GFAtomicExpression> guardedsAll;
	protected Set<Pair<GFAtomicExpression, GFAtomicExpression>> ggpairsAll;

	protected GFAtomicExpression[] atoms;

	private DirManager dirManager;

	/**
	 * 
	 */
	public ExpressionSetOperations() {
		guardeds = new HashMap<>();
		booleanChildExpressions = new HashMap<>();
		projections = new HashMap<>();

		guardHasProjection = new HashMap<>();
		guardHasGuard = new HashMap<>();

		guardsAll = new HashSet<>();
		guardedsAll = new HashSet<>();
		ggpairsAll = new HashSet<>();
	}

	public void setExpressionSet(Collection<GFExistentialExpression> expressionSet) throws GFOperationInitException {
		this.expressionSet = expressionSet;
		preCalculate();
	}

	private void preCalculate() throws GFOperationInitException {

		// precalculate guarded relations for each formula
		guardeds.clear();

		for (GFExistentialExpression e : expressionSet) {
			Collection<GFAtomicExpression> guardedsOfE = e.getGuardedRelations();
			guardeds.put(e, guardedsOfE);
		}

		// precalculate boolean expression for each formula
		booleanChildExpressions.clear();

		GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
		for (GFExistentialExpression e : expressionSet) {

			try {
				BExpression booleanChildExpression = convertor.convert(e.getChild());

				GFBooleanMapping mapGFtoB = convertor.getMapping();

				booleanChildExpressions.put(e, booleanChildExpression);

			} catch (GFtoBooleanConversionException e1) {

				LOG.error("Something went wrong when converting GF to boolean: " + e);
				throw new GFOperationInitException(e1);
			}
		}
		booleanMapping = convertor.getMapping();

		// precalculate mappings
		projections.clear();
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression output = e.getOutputRelation();
			GFAtomicExpression guard = e.getGuard();
			GFAtomProjection p = new GFAtomProjection(guard, output);
			projections.put(e, p);
		}

		// pairs and unions
		guardsAll.clear();
		guardedsAll.clear();
		ggpairsAll.clear();
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression guard = e.getGuard();
			guardsAll.add(guard);

			for (GFAtomicExpression c : e.getGuardedRelations()) {
				guardedsAll.add(c);
				ggpairsAll.add(new Pair<>(guard, c));
			}

		}

		// map between guards and guardeds
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression guard = e.getGuard();

			Set<GFAtomicExpression> set;
			if (guardHasGuard.containsKey(guard)) {
				set = guardHasGuard.get(guard);
			} else {
				set = new HashSet<>();
				guardHasGuard.put(guard, set);
			}

			for (GFAtomicExpression c : e.getGuardedRelations()) {
				set.add(c);
			}
		}

		// map between guards and projections
		for (Pair<GFAtomicExpression, GFAtomicExpression> p : ggpairsAll) {

			GFAtomicExpression guard = p.fst;
			GFAtomicExpression guarded = p.snd;

			GFAtomProjection r = new GFAtomProjection(guard, guarded);

			guardHasProjection.put(p, r);

		}

		// order the atoms
		HashSet<GFAtomicExpression> allAtoms = new HashSet<>(guardsAll);
		allAtoms.addAll(guardedsAll);

		atoms = allAtoms.toArray(new GFAtomicExpression[0]);
		Arrays.sort(atoms);
//		for (int i = 0; i < atoms.length; i++) {
//			GFAtomicExpression atom = atoms[i];
//			LOG.info("" + i + ": " + atom);
//		}

	}

	public Set<GFAtomicExpression> getGuardeds(GFAtomicExpression guard) throws GFOperationInitException {
		Set<GFAtomicExpression> r = guardHasGuard.get(guard);

		if (r == null)
			throw new GFOperationInitException("No guardeds found for: " + guard);

		return r;
	}

	public GFAtomProjection getProjections(GFAtomicExpression guard, GFAtomicExpression guarded)
			throws GFOperationInitException {

		GFAtomProjection r = guardHasProjection.get(new Pair<>(guard, guarded));

		if (r == null)
			throw new GFOperationInitException("No projections found for: " + guard + " " + guarded);

		return r;
	}

	public Collection<GFAtomicExpression> getGuardeds(GFExistentialExpression e) throws GFOperationInitException {

		Collection<GFAtomicExpression> g = guardeds.get(e);

		if (g == null)
			throw new GFOperationInitException("No guarded relations found for: " + e);

		return g;
	}

	public BExpression getBooleanChildExpression(GFExistentialExpression e) throws GFOperationInitException {
		BExpression b = booleanChildExpressions.get(e);

		if (b == null)
			throw new GFOperationInitException("No boolean formula found for: " + e);

		return b;
	}

	/**
	 * A common boolean mapping used for ALL formulas in this set.
	 * 
	 * @return a mapping between atoms and variables
	 * @throws GFOperationInitException
	 */
	public GFBooleanMapping getBooleanMapping() throws GFOperationInitException {
		return booleanMapping;
	}

	public GFAtomProjection getOutputProjection(GFExistentialExpression e) throws GFOperationInitException {
		GFAtomProjection p = projections.get(e);

		if (p == null)
			throw new GFOperationInitException("No projection found for: " + e);

		return p;
	}

	/**
	 * @return the set of all guarded relations
	 */
	public Set<GFAtomicExpression> getGuardedsAll() {
		return guardedsAll;
	}

	/**
	 * @return the set of all guards
	 */
	public Set<GFAtomicExpression> getGuardsAll() {
		return guardsAll;
	}

	/**
	 * Returns the set of all guard-guarded combinations, based on the
	 * GFExistentialExpressions
	 * 
	 * @return the set of all guard-guarded combinations
	 */
	public Set<Pair<GFAtomicExpression, GFAtomicExpression>> getGGPairsAll() {
		return ggpairsAll;
	}

	public Collection<GFExistentialExpression> getExpressionSet() {
		return expressionSet;
	}

	/**
	 * @param dirManager
	 */
	public void setDirManager(DirManager dirManager) {
		this.dirManager = dirManager;

	}

	/**
	 * @return the input paths for the guarded relations
	 */
	public Set<Path> getGuardedPaths() {
		HashSet<Path> result = new HashSet<Path>();

		for (GFAtomicExpression guarded : getGuardedsAll()) {
			Set<Path> paths = dirManager.lookup(guarded.getRelationSchema());
			result.addAll(paths);
		}

		return result;
	}

	/**
	 * 
	 * @return the input paths for the guard relations
	 */
	public Set<Path> getGuardPaths() {
		HashSet<Path> result = new HashSet<Path>();

		for (GFAtomicExpression guarded : getGuardsAll()) {
			Set<Path> paths = dirManager.lookup(guarded.getRelationSchema());
			result.addAll(paths);
		}

		return result;
	}

	/**
	 * Fetches an atom with the given internal id. Id is only unique for THIS
	 * set of expressions.
	 * 
	 * @param id
	 *            the id of the atom
	 * @return the atom with the given id
	 */
	public GFAtomicExpression getAtom(int id) throws GFOperationInitException {
		if (0 <= id && id < atoms.length)
			return atoms[id];
		else
			throw new GFOperationInitException("Atom with not found: id " + id);
	}

	/**
	 * Fetches the internal id of a given atom. Id is only unique for THIS
	 * set of expressions.
	 * 
	 * @param atom
	 *            the atom to look up
	 * @return the id of the given atom
	 */
	public int getAtomId(GFAtomicExpression atom) throws GFOperationInitException {

		for (int i = 0; i < atoms.length; i++) {
			if (atoms[i].equals(atom)) {
				return i;
			}
		}
		
		throw new GFOperationInitException("Atom with not found: " + atom);
	}

	/**
	 * @return the guarded paths that are in rel format
	 */
	public Set<Path> getGuardedRelPaths() {
		Set<Path> paths = dirManager.getFileMapping().getPathsWithFormat(InputFormat.REL);
		paths.retainAll(getGuardedPaths());
		return paths;
	}
	
	/**
	 * @return the guarded paths that are in csv format
	 */
	public Set<Path> getGuardedCsvPaths() {
		Set<Path> paths = dirManager.getFileMapping().getPathsWithFormat(InputFormat.CSV);
		paths.retainAll(getGuardedPaths());
		return paths;
	}

	/**
	 * @return the guarded paths that are in rel format
	 */
	public Set<Path> getGuardRelPaths() {
		Set<Path> paths = dirManager.getFileMapping().getPathsWithFormat(InputFormat.REL);
		paths.retainAll(getGuardPaths());
		return paths;
	}
	
	/**
	 * @return the guarded paths that are in csv format
	 */
	public Set<Path> getGuardCsvPaths() {
		Set<Path> paths = dirManager.getFileMapping().getPathsWithFormat(InputFormat.CSV);
		paths.retainAll(getGuardPaths());
		return paths;
	}
}
