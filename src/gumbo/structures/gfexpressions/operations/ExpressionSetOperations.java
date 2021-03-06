/**
 * Created: 25 Sep 2014
 */
package gumbo.structures.gfexpressions.operations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.conversion.GFtoBooleanConversionException;
import gumbo.structures.conversion.GFtoBooleanConvertor;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.io.Triple;

/**
 * Wrapper for a set of GF Expressions.
 * Offers a set of useful operations for which the result is precalculated.
 * This is useful when operations are called often, e.g., in a mapper or reducer:
 * The internal caching system will then avoid repeated calculations.
 * 
 * CLEAN this class needs to be redesigned and rewritten
 * 
 * @author Jonny Daenen
 * 
 */
public class ExpressionSetOperations implements Externalizable {

	public class GFOperationInitException extends Exception {

		private static final long serialVersionUID = 1L;

		public GFOperationInitException(String msg) {
			super(msg);
		}

		public GFOperationInitException(String msg, Exception e) {
			super(msg,e);
		}
	}

	private static final Log LOG = LogFactory.getLog(ExpressionSetOperations.class);

	protected Collection<GFExistentialExpression> expressionSet;
	private RelationFileMapping fileMapping;

	protected HashMap<GFExistentialExpression, Collection<GFAtomicExpression>> guardeds;
	protected HashMap<GFExistentialExpression, BExpression> booleanChildExpressions;
	protected GFBooleanMapping booleanMapping;
	protected HashMap<GFExistentialExpression, GFAtomProjection> projections;

	protected HashMap<Pair<GFAtomicExpression, GFAtomicExpression>, GFAtomProjection> guardHasProjection;
	protected HashMap<GFAtomicExpression, HashMap<GFAtomicExpression, GFAtomProjection>> guardHasProjection2;
	protected HashMap<GFAtomicExpression, Set<GFAtomicExpression>> guardHasGuard;
	protected HashMap<GFAtomicExpression, Set<Triple<GFAtomicExpression,GFAtomProjection,Integer>>> guardHasGuardAndProjection;

	protected Set<GFAtomicExpression> guardsAll;
	protected Set<GFAtomicExpression> guardedsAll;
	protected Set<Pair<GFAtomicExpression, GFAtomicExpression>> ggpairsAll;

	protected GFAtomicExpression[] atoms;

	protected HashMap<GFAtomicExpression, Integer> atomIDs;

	private Collection<GFExistentialExpression> allExpressions;


	private ExpressionSetOperations() {
		guardeds = new HashMap<>();
		booleanChildExpressions = new HashMap<>();
		projections = new HashMap<>();

		guardHasProjection = new HashMap<>();
		guardHasProjection2 = new HashMap<>();
		guardHasGuard = new HashMap<>();
		guardHasGuardAndProjection = new HashMap<>();

		guardsAll = new HashSet<>();
		guardedsAll = new HashSet<>();
		ggpairsAll = new HashSet<>();

		atomIDs = new HashMap<>();
	}

	/**
	 * 
	 * @param expressionSet subset of expressions that is actually used
	 * @param allExpressions set of all expressions in this round
	 * @param fileMapping the file mapping (at least the used expressions)
	 * @throws GFOperationInitException
	 */
	public ExpressionSetOperations(Collection<GFExistentialExpression> expressionSet,Collection<GFExistentialExpression> allExpressions , RelationFileMapping fileMapping) throws GFOperationInitException {
		this();
		this.fileMapping = fileMapping;
		setExpressionSet(expressionSet,allExpressions);
	}

	public void setExpressionSet(Collection<GFExistentialExpression> expressionSet,Collection<GFExistentialExpression> allExpressions) throws GFOperationInitException {
		this.expressionSet = expressionSet;
		this.allExpressions = allExpressions;
		preCalculate();
	}

	private void preCalculate() throws GFOperationInitException {

		// precalculate guarded relations for each formula
		guardeds.clear();

		for (GFExistentialExpression e : expressionSet) {
			Collection<GFAtomicExpression> guardedsOfE = e.getGuardedAtoms();
			guardeds.put(e, guardedsOfE);
		}

		// precalculate boolean expression for each formula
		booleanChildExpressions.clear();

		GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
		for (GFExistentialExpression e : expressionSet) {

			try {
				BExpression booleanChildExpression = convertor.convert(e.getChild());

				booleanChildExpressions.put(e, booleanChildExpression);

			} catch (GFtoBooleanConversionException e1) {

				LOG.error("Something went wrong when converting GF to boolean: " + e);
				throw new GFOperationInitException("Problem when during GF-boolean conversion: " + e,e1);
			}
		}
		// get mapping of ALL formulas
		booleanMapping = convertor.getMapping(); 

		// precalculate mappings from guard to the output relation
		projections.clear();
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression output = e.getOutputRelation();
			GFAtomicExpression guard = e.getGuard();
			GFAtomProjection p = new GFAtomProjection(guard, output);
			projections.put(e, p);
		}

		// collect all the guards, guardeds and their combinations
		guardsAll.clear();
		guardedsAll.clear();
		ggpairsAll.clear();
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression guard = e.getGuard();
			guardsAll.add(guard);

			for (GFAtomicExpression c : e.getGuardedAtoms()) {
				guardedsAll.add(c); // TODO what if an atom is both?
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

			for (GFAtomicExpression c : e.getGuardedAtoms()) {
				set.add(c);
			}
		}


		// map between guards and projections
		// for each pair the projection is cached
		for (Pair<GFAtomicExpression, GFAtomicExpression> p : ggpairsAll) {

			GFAtomicExpression guard = p.fst;
			GFAtomicExpression guarded = p.snd;

			GFAtomProjection r = new GFAtomProjection(guard, guarded);

			guardHasProjection.put(p, r); 

			// improvement
			HashMap<GFAtomicExpression, GFAtomProjection> map = null;
			if (guardHasProjection2.containsKey(guarded)) {
				map = guardHasProjection2.get(guarded);
			} else {
				map = new HashMap<GFAtomicExpression, GFAtomProjection>();
				guardHasProjection2.put(guarded, map);
			}
			map.put(guard, r);	

		}

		// sort the atoms to obtain an ordering
		HashSet<GFAtomicExpression> allAtoms = new HashSet<>(20);
		for (GFExistentialExpression exp : allExpressions) {
//			System.out.println(exp);
			Collection<GFAtomicExpression> atomics = exp.getAtomic();
			allAtoms.addAll(atomics);
		}

		atoms = allAtoms.toArray(new GFAtomicExpression[0]);
		Arrays.sort(atoms);
//		LOG.info("ATOM IDS:" + Arrays.toString(atoms));
		atomIDs.clear();
		for (int i = 0; i < atoms.length; i++) {
			atomIDs.put(atoms[i], i);
		}

		// map between guards and guardeds + projection
		for (GFExistentialExpression e : expressionSet) {
			GFAtomicExpression guard = e.getGuard();

			Set<Triple<GFAtomicExpression, GFAtomProjection,Integer>> set;
			if (guardHasGuardAndProjection.containsKey(guard)) {
				set = guardHasGuardAndProjection.get(guard);
			} else {
				set = new HashSet<>();
				guardHasGuardAndProjection.put(guard, set);
			}

			for (GFAtomicExpression c : e.getGuardedAtoms()) {
				GFAtomProjection r = new GFAtomProjection(guard, c);
				Triple<GFAtomicExpression, GFAtomProjection, Integer> pair = new Triple<>(c, r, atomIDs.get(c));
				set.add(pair);
			}
		}

	}

	/**
	 * Returns a set of all guarded atoms and the precaltulated projections onto them that appear below a given guard.
	 * 
	 * @param guard the guard
	 * 
	 * @return the set of all guarded atoms and precalculated projections guarded by the specified guard 
	 * 
	 * @throws GFOperationInitException
	 */
	public Set<Triple<GFAtomicExpression,GFAtomProjection,Integer>> getGuardedsAndProjections(GFAtomicExpression guard) throws GFOperationInitException {
		Set<Triple<GFAtomicExpression,GFAtomProjection,Integer>> r = guardHasGuardAndProjection.get(guard);

		if (r == null) // TODO empty set? 
			throw new GFOperationInitException("No guardeds found for: " + guard);

		return r;
	}

	/**
	 * Creates a set of all guarded atom that appear below a given guard.
	 * 
	 * @param guard the guard
	 * 
	 * @return the set of all guarded atoms guarded by the specified guard
	 * 
	 * @throws GFOperationInitException
	 */
	public Set<GFAtomicExpression> getGuardeds(GFAtomicExpression guard) throws GFOperationInitException {
		Set<GFAtomicExpression> r = guardHasGuard.get(guard);

		if (r == null) // TODO empty set? 
			throw new GFOperationInitException("No guardeds found for: " + guard);

		return r;
	}

	private Pair<GFAtomicExpression, GFAtomicExpression> comparisonObject = new Pair<>(null,null);

	/**
	 * Returns a projection to transform a guard tuple into a guarded tuple.
	 * 
	 * @param guard the guard atom
	 * @param guarded the guarded atom
	 * 
	 * @return a projection from guard to guarded
	 * 
	 * @throws GFOperationInitException
	 */
	public GFAtomProjection getProjections(GFAtomicExpression guard, GFAtomicExpression guarded)
			throws GFOperationInitException {
		//		comparisonObject.fst = guard;
		//		comparisonObject.snd = guarded;
		//		GFAtomProjection r = guardHasProjection.get(comparisonObject);
		//
		//		if (r == null)
		//			throw new GFOperationInitException("No projections found for: " + guard + " " + guarded);
		//
		//		return r;

		HashMap<GFAtomicExpression, GFAtomProjection> map = guardHasProjection2.get(guarded);
		if (map == null)
			throw new GFOperationInitException("No projections found for: " + guard + " " + guarded);

		GFAtomProjection item = map.get(guard);

		if (item == null)
			throw new GFOperationInitException("No projections found for: " + guard + " " + guarded);

		return item;
	}

	/**
	 * Returns the set of guarded atoms that belong to a GF expression.
	 * 
	 * @param e a GF expression
	 * 
	 * @return the set of guardeds belonging to a GF expression
	 * 
	 * @throws GFOperationInitException
	 */
	public Collection<GFAtomicExpression> getGuardeds(GFExistentialExpression e) throws GFOperationInitException {

		Collection<GFAtomicExpression> g = guardeds.get(e);

		if (g == null)
			throw new GFOperationInitException("No guarded relations found for: " + e);

		return g;
	}

	/**
	 * Returns a boolean expression corresponding to the guarded part of the GF expression.
	 * 
	 * @param e a GF expression
	 * 
	 * @return the guarded part as a boolean expression
	 * 
	 * @throws GFOperationInitException
	 */
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
	 * 
	 * @throws GFOperationInitException
	 */
	public GFBooleanMapping getBooleanMapping() throws GFOperationInitException {
		return booleanMapping;
	}

	/**
	 * Returns a mapping from guard to output relation
	 * 
	 * @param e the GF expression
	 * 
	 * @return a mapping from guard to output relation
	 * 
	 * @throws GFOperationInitException
	 */
	public GFAtomProjection getOutputProjection(GFExistentialExpression e) throws GFOperationInitException {
		GFAtomProjection p = projections.get(e);

		if (p == null)
			throw new GFOperationInitException("No projection found for: " + e);

		return p;
	}

	/**
	 * Returns the set of all guarded atoms used in the GF expressions.
	 * TODO also the ones that appear as guard?
	 * 
	 * @return the set of all guarded relations
	 */
	public Set<GFAtomicExpression> getGuardedsAll() {
		return guardedsAll;
	}

	/**
	 * Returns the set of all guard atoms used in the GF expressions.
	 * @return the set of all guards
	 */
	public Set<GFAtomicExpression> getGuardsAll() {
		return guardsAll;
	}

	/**
	 * Returns the set of all guard-guarded combinations.
	 * TODO also the guards that appear as guard?
	 * 
	 * @return the set of all guard-guarded combinations
	 */
	public Set<Pair<GFAtomicExpression, GFAtomicExpression>> getGGPairsAll() {
		return ggpairsAll;
	}

	/**
	 * Returns the internal expression set.
	 * 
	 * @return the expressionset that is used
	 */
	public Collection<GFExistentialExpression> getExpressionSet() {
		return expressionSet;
	}


	/**
	 * Constructs the set of {@link Path}s that serve as guarded inputs.
	 * 
	 * @return the set of input paths for the guarded relations
	 */
	public Set<Path> getGuardedPaths() {
		HashSet<Path> result = new HashSet<Path>();

		for (GFAtomicExpression guarded : getGuardedsAll()) {
			Set<Path> paths = fileMapping.getPaths(guarded.getRelationSchema());
			result.addAll(paths);
		}

		return result;
	}

	/**
	 * Constructs the set of {@link Path}s that serve as guard inputs.
	 * 
	 * @return the input paths for the guard relations
	 */
	public Set<Path> getGuardPaths() {
		HashSet<Path> result = new HashSet<Path>();

		for (GFAtomicExpression guard : getGuardsAll()) {
			Set<Path> paths = fileMapping.getPaths(guard.getRelationSchema());
			result.addAll(paths);
		}

		return result;
	}

	/**
	 * Fetches an atom with the given internal id. 
	 * <b>Warning:</b> the id is only unique for THIS set of expressions.
	 * 
	 * @param id the atom id
	 *             
	 * @return the atom with the given id
	 */
	public GFAtomicExpression getAtom(int id) throws GFOperationInitException {
		if (0 <= id && id < atoms.length)
			return atoms[id];
		else
			throw new GFOperationInitException("Atom with not found: id " + id);
	}

	/**
	 * Fetches the internal id of a given atom. 
	 * <b>Warning:</b> the id is only unique for THIS set of expressions.
	 * 
	 * @param atom the atom
	 * 
	 * @return the id of the given atom
	 */
	public int getAtomId(GFAtomicExpression atom) throws GFOperationInitException {

		// this seems to be faster than the hashset.. 
		for (int i = 0; i < atoms.length; i++) {
			if (atoms[i].equals(atom)) {
				return i;
			}
		}
		//		Integer result = atomdIDs.get(atom);
		//		
		//		if (result == null)
		throw new GFOperationInitException("Atom not found: " + atom);

		//		return result;
	}

	/**
	 * @return the guarded paths that are in rel format
	 */
	public Set<Path> getGuardedRelPaths() {
		Set<Path> paths = fileMapping.getPathsWithFormat(InputFormat.REL);
		paths.retainAll(getGuardedPaths());
		return paths;
	}

	/**
	 * @return the guarded paths that are in csv format
	 */
	public Set<Path> getGuardedCsvPaths() {
		Set<Path> paths = fileMapping.getPathsWithFormat(InputFormat.CSV);
		paths.retainAll(getGuardedPaths());
		return paths;
	}

	/**
	 * @return the guarded paths that are in rel format
	 */
	public Set<Path> getGuardRelPaths() {
		Set<Path> paths = fileMapping.getPathsWithFormat(InputFormat.REL);
		paths.retainAll(getGuardPaths());
		return paths;
	}

	/**
	 * @return the guarded paths that are in csv format
	 */
	public Set<Path> getGuardCsvPaths() {
		Set<Path> paths = fileMapping.getPathsWithFormat(InputFormat.CSV);
		paths.retainAll(getGuardPaths());
		return paths;
	}

	/**
	 * Creates the set of paths that appear in both as guarded and as guard.
	 * 
	 * @return the set of paths that are both guard and guarded.
	 */
	public Collection<Path> intersectGuardGuardedPaths() {
		Set<Path> a1 = getGuardedRelPaths();
		Set<Path> a2 = getGuardRelPaths();

		Set<Path> b1 = getGuardedCsvPaths();
		Set<Path> b2 = getGuardCsvPaths();

		// union guarded
		Set<Path> a = new HashSet<>(a1);
		a.addAll(b1);

		// union guard
		Set<Path> b = new HashSet<>(a2);
		b.addAll(b2);

		// intersect
		a.retainAll(b);

		return a;
	}

	/**
	 * Returns the internal file mapping.
	 * 
	 * @return the internal file mapping
	 */
	public RelationFileMapping getFileMapping() {
		return fileMapping;
	}


	/* (non-Javadoc)
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(expressionSet);
		out.writeObject(fileMapping);
	}


	/* (non-Javadoc)
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		expressionSet = (Collection<GFExistentialExpression>) in.readObject();
		fileMapping = (RelationFileMapping) in.readObject();

	}

	public boolean isGuard(RelationSchema r) {
		
		for (GFAtomicExpression guard : guardsAll) {
			if (guard.getRelationSchema().equals(r))
				return true;
		}
		return false;
	}




}
