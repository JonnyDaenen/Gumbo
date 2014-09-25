/**
 * Created: 25 Sep 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mapreduce.guardedfragment.planner.GFMRPlannerException;
import mapreduce.guardedfragment.planner.compiler.reducers.GFReducer2Generic;
import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConversionException;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConvertor;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;

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

	/**
	 * 
	 */
	public ExpressionSetOperation() {
		guardeds = new HashMap<>();
		booleanChildExpressions = new HashMap<>();
		booleanMapping = new HashMap<>();
		projections = new HashMap<>();
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
	
	protected GFAtomProjection getProjection(GFExistentialExpression e) throws GFOperationInitException {
		GFAtomProjection p = projections.get(e);

		if (p == null)
			throw new GFOperationInitException("No projection found for: " + e);

		return p;
	}

	public Collection<GFExistentialExpression> getExpressionSet() {
		return expressionSet;
	}

}
