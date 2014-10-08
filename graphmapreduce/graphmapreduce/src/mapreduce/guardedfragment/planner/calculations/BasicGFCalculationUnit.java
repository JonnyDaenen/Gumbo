/**
 * Created: 28 Apr 2014
 */
package mapreduce.guardedfragment.planner.calculations;

import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;

/**
 * @author Jonny Daenen
 *
 */
public class BasicGFCalculationUnit extends CalculationUnit {
	
	GFExistentialExpression basicExpression;
	
	public BasicGFCalculationUnit(int id, GFExistentialExpression basicExpression ) throws CalculationUnitException {
		super(id);
		if(!basicExpression.isBasicGF())
			throw new CalculationUnitException("Supplied expression is not basic");
		
		this.basicExpression = basicExpression;
	}
	
	@Deprecated
	public BasicGFCalculationUnit(GFExistentialExpression basicExpression ) throws CalculationUnitException {
		if(!basicExpression.isBasicGF())
			throw new CalculationUnitException("Supplied expression is not basic");
		
		this.basicExpression = basicExpression;
	}





	/**
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnit#getNumRounds()
	 */
	@Override
	int getNumRounds() {
		return 2;
	}


	/**
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnit#getOutputSchema()
	 */
	@Override
	public RelationSchema getOutputSchema() {
		return basicExpression.getOutputSchema();
	}

	
	/**
	 * @return the basicExpression
	 */
	public GFExistentialExpression getBasicExpression() {
		return basicExpression;
	}
	
	/**
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnit#toString()
	 */
	@Override
	public String toString() {
		return super.toString() + " - " + basicExpression.toString() ;
	}

	/**
	 * @see mapreduce.guardedfragment.planner.calculations.CalculationUnit#getInputRelations()
	 */
	@Override
	public Set<RelationSchema> getInputRelations() {
		return basicExpression.getRelationDependencies();
	}

}
