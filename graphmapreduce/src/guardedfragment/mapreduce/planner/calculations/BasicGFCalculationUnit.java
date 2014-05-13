/**
 * Created: 28 Apr 2014
 */
package guardedfragment.mapreduce.planner.calculations;

import guardedfragment.structure.gfexpressions.GFExistentialExpression;

import java.util.Set;

import mapreduce.data.RelationSchema;

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
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getNumRounds()
	 */
	@Override
	int getNumRounds() {
		return 2;
	}


	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getOutputSchema()
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
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#toString()
	 */
	@Override
	public String toString() {
		return super.toString() + " - " + basicExpression.toString() ;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.calculations.CalculationUnit#getInputRelations()
	 */
	@Override
	public Set<RelationSchema> getInputRelations() {
		return basicExpression.getRelationDependencies();
	}

}
