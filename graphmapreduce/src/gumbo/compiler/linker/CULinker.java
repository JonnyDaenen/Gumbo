/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.linker;

import gumbo.compiler.GFCompilerException;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Links {@link CalculationUnit}s in a DAG.
 * TODO #core change to GFBasicExpression
 * 
 * 
 * @author Jonny Daenen
 * 
 */
public class CULinker {


	private static final Log LOG = LogFactory.getLog(CULinker.class);



	public CalculationUnitGroup createDAG(Map<RelationSchema, BasicGFCalculationUnit> basics) {


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
		
		

		CalculationUnitGroup partition = new CalculationUnitGroup();
		for (CalculationUnit c : basics.values()) {
			partition.add(c);
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
	 * @param basics
	 * @param cu
	 * @throws GFCompilerException
	 */
	private void addGfe(Map<RelationSchema, BasicGFCalculationUnit> basics, BasicGFCalculationUnit cu)
			throws GFCompilerException {
		// TODO fix duplicate schema's here!
		if (basics.containsKey(cu.getOutputSchema()))
			throw new GFCompilerException("duplicate intermediate relation schema");

		basics.put(cu.getOutputSchema(), cu);

	}
}
