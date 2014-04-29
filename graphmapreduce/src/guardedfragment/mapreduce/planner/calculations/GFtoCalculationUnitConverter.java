/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner.calculations;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import guardedfragment.mapreduce.GFMRPlannerException;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.operations.GFDecomposer;
import guardedfragment.structure.gfexpressions.operations.GFDecomposerException;
import mapreduce.MRPlan;
import mapreduce.data.RelationSchema;

/**
 * New version of the planner, supports multiple ranks.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFtoCalculationUnitConverter {

	GFDecomposer decomposer;

	private static final Log LOG = LogFactory.getLog(GFtoCalculationUnitConverter.class);

	/**
	 * 
	 */
	public GFtoCalculationUnitConverter() {
		decomposer = new GFDecomposer();
	}

	CalculationPartition createCalculationUnits(Set<GFExistentialExpression> gfeset) {

		// convert all non-basic expressions to basic
		Map<RelationSchema, BasicGFCalculationUnit> basics = toBasic(gfeset);

		// determine all relations that appear
		Set<RelationSchema> relations = getRelations(basics);
		
		Set<RelationSchema> inputRelations = new HashSet<RelationSchema>();
		Set<RelationSchema> intermediateRelations = new HashSet<RelationSchema>();
		
		// link dependent calculation units
		
		for (BasicGFCalculationUnit cu : basics.values()) {
			
			// link dependencies
			Set<RelationSchema> depRelations = cu.getBasicExpression().getRelationDependencies();
			for (RelationSchema rs : depRelations) {
				
				// if no dependency found, it is an input relation
				if(!basics.containsKey(rs)) { 
					inputRelations.add(rs);
					cu.addInputRelation(rs);
				}
				// otherwise, find and link dependency
				else {
					
					intermediateRelations.add(rs);
					BasicGFCalculationUnit cuDep = basics.get(rs);
					cu.setDependency(rs, cuDep);
				}
				
			}
			
		}
		
		// TODO check for cyclic dependencies
		
		

		CalculationPartition partition = new CalculationPartition();
		for (CalculationUnit c : basics.values()) {
			partition.addCalculation(c);
		}
		
		return partition;
	}

	/**
	 * @param basics
	 * @return all the used relations in the expression
	 */
	private Set<RelationSchema> getRelations(Map<RelationSchema, BasicGFCalculationUnit> basics) {
		Set<RelationSchema> relations = new HashSet<RelationSchema>();

		for (BasicGFCalculationUnit cu : basics.values()) {
			GFExistentialExpression gfe = cu.getBasicExpression();
			Set<RelationSchema> current = gfe.getRelationDependencies();
			relations.addAll(current);
		}

		return relations;
	}

	/**
	 * @param gfeset
	 * @return
	 */
	private Map<RelationSchema, BasicGFCalculationUnit> toBasic(Set<GFExistentialExpression> gfeset) {
		Map<RelationSchema, BasicGFCalculationUnit> basics = new HashMap<RelationSchema, BasicGFCalculationUnit>();

		for (GFExistentialExpression gfe : gfeset) {
			try {
				// split up into basics if necessary
				if (!gfe.isAtomicBooleanCombination()) {

					Set<GFExistentialExpression> current = decomposer.decompose(gfe);

					for (GFExistentialExpression newGfe : current) {
						BasicGFCalculationUnit cu = new BasicGFCalculationUnit(newGfe);
						addGfe(basics, cu);
					}

				} else {
					BasicGFCalculationUnit cu = new BasicGFCalculationUnit(gfe);
					addGfe(basics, cu);
				}
			} catch (Exception e) {
				LOG.error("Skipping formula because of error: " + gfe + ". Error message: "
						+ e.getMessage());
			}
		}

		return basics;
	}

	/**
	 * @param basics
	 * @param cu
	 * @throws GFMRPlannerException
	 */
	private void addGfe(Map<RelationSchema, BasicGFCalculationUnit> basics, BasicGFCalculationUnit cu)
			throws GFMRPlannerException {
		// TODO fix duplicate schema's here!
		if (basics.containsKey(cu.getOutputSchema()))
			throw new GFMRPlannerException("duplicate intermediate relation schema");

		basics.put(cu.getOutputSchema(), cu);

	}
}
