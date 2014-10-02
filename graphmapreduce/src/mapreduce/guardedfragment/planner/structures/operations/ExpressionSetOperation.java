/**
 * Created: 25 Sep 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConversionException;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConvertor;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 * 
 */
public abstract class ExpressionSetOperation {

	private static final Log LOG = LogFactory.getLog(ExpressionSetOperation.class);

	protected Collection<GFExistentialExpression> expressionSet;

	protected HashMap<GFExistentialExpression, Collection<GFAtomicExpression>> guardeds;
	protected HashMap<GFExistentialExpression, BExpression> booleanChildExpressions;
	protected HashMap<GFExistentialExpression, GFBooleanMapping> booleanMapping;
	protected HashMap<GFExistentialExpression, GFAtomProjection> projections;

	protected HashMap<Pair<GFAtomicExpression, GFAtomicExpression>, GFAtomProjection> guardHasProjection;
	protected HashMap<GFAtomicExpression, Set<GFAtomicExpression>> guardHasGuard;

	protected Set<GFAtomicExpression> guardsAll;
	protected Set<GFAtomicExpression> guardedsAll;
	protected Set<Pair<GFAtomicExpression, GFAtomicExpression>> ggpairsAll;

	/**
	 * 
	 */
	public ExpressionSetOperation() {
		guardeds = new HashMap<>();
		booleanChildExpressions = new HashMap<>();
		booleanMapping = new HashMap<>();
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
		booleanMapping.clear();

		GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
		for (GFExistentialExpression e : expressionSet) {

			try {
				BExpression booleanChildExpression = convertor.convert(e.getChild());

				GFBooleanMapping mapGFtoB = convertor.getMapping();

				booleanChildExpressions.put(e, booleanChildExpression);
				booleanMapping.put(e, mapGFtoB);

			} catch (GFtoBooleanConversionException e1) {

				LOG.error("Something went wrong when converting GF to boolean: " + e);
				throw new GFOperationInitException(e1);
			}
		}

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

	}

	protected Set<GFAtomicExpression> getGuardeds(GFAtomicExpression guard) throws GFOperationInitException {
		Set<GFAtomicExpression> r = guardHasGuard.get(guard);

		if (r == null)
			throw new GFOperationInitException("No guardeds found for: " + guard);

		return r;
	}

	protected GFAtomProjection getProjections(GFAtomicExpression guard, GFAtomicExpression guarded) throws GFOperationInitException {
		
		GFAtomProjection r = guardHasProjection.get(new Pair<>(guard,guarded));

		if (r == null)
			throw new GFOperationInitException("No projections found for: " + guard + " " + guarded);

		return r;
	}

	protected Collection<GFAtomicExpression> getGuardeds(GFExistentialExpression e) throws GFOperationInitException {

		Collection<GFAtomicExpression> g = guardeds.get(e);

		if (g == null)
			throw new GFOperationInitException("No guarded relations found for: " + e);

		return g;
	}

	protected BExpression getBooleanChildExpression(GFExistentialExpression e) throws GFOperationInitException {
		BExpression b = booleanChildExpressions.get(e);

		if (b == null)
			throw new GFOperationInitException("No boolean formula found for: " + e);

		return b;
	}

	protected GFBooleanMapping getBooleanMapping(GFExistentialExpression e) throws GFOperationInitException {
		GFBooleanMapping m = booleanMapping.get(e);

		if (m == null)
			throw new GFOperationInitException("No boolean mapping found for: " + e);

		return m;
	}

	protected GFAtomProjection getOutputProjection(GFExistentialExpression e) throws GFOperationInitException {
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

}
