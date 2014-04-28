/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner;

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
public class GFMRPlanner {

	GFDecomposer decomposer;

	private static final Log LOG = LogFactory.getLog(GFMRPlanner.class);

	/**
	 * 
	 */
	public GFMRPlanner() {
		decomposer = new GFDecomposer();
	}

	MRPlan createPlan(Set<GFExistentialExpression> gfeset) {

		// convert all non-basic expressions to basic
		Map<RelationSchema, GFExistentialExpression> basics = toBasic(gfeset);

		// determine all relations that appear
		Set<RelationSchema> relations = getRelations(basics);
		
		// link dependent calculation units
		// TODO implement
		
		

		MRPlan plan = new MRPlan();
		return plan;
	}

	/**
	 * @param basics
	 * @return
	 */
	private Set<RelationSchema> getRelations(Map<RelationSchema, GFExistentialExpression> basics) {
		Set<RelationSchema> relations = new HashSet<RelationSchema>();

		for (GFExistentialExpression gfe : basics.values()) {
			Set<RelationSchema> current = gfe.getRelationDependencies();
			relations.addAll(current);
		}

		return relations;
	}

	/**
	 * @param gfeset
	 * @return
	 */
	private Map<RelationSchema, GFExistentialExpression> toBasic(Set<GFExistentialExpression> gfeset) {
		Map<RelationSchema, GFExistentialExpression> basics = new HashMap<RelationSchema, GFExistentialExpression>();

		for (GFExistentialExpression gfe : gfeset) {
			try {
				// split up into basics if necessary
				if (!gfe.isAtomicBooleanCombination()) {

					Set<GFExistentialExpression> current = decomposer.decompose(gfe);

					for (GFExistentialExpression newGfe : current)
						addGfe(basics, newGfe);

				} else
					addGfe(basics, gfe);
			} catch (Exception e) {
				LOG.error("Skipping formula because of decomposition error: " + gfe + ". Error message: "
						+ e.getMessage());
			}
		}

		return basics;
	}

	/**
	 * @param basics
	 * @param gfe
	 * @throws GFMRPlannerException
	 */
	private void addGfe(Map<RelationSchema, GFExistentialExpression> basics, GFExistentialExpression gfe)
			throws GFMRPlannerException {
		// TODO fix duplicate schema's here!
		if (basics.containsKey(gfe.getOutputSchema()))
			throw new GFMRPlannerException("duplicate intermediate relation schema");

		basics.put(gfe.getOutputSchema(), gfe);

	}
}
