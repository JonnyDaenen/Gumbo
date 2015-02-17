/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.calculations;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.Set;

/**
 * Represents 1-level GF expressions.
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
	
	public BasicGFCalculationUnit(GFExistentialExpression basicExpression ) throws CalculationUnitException {
		if(!basicExpression.isBasicGF())
			throw new CalculationUnitException("Supplied expression is not basic");
		
		this.basicExpression = basicExpression;
	}






	/**
	 * @see gumbo.compiler.calculations.CalculationUnit#getOutputSchema()
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
	 * @see gumbo.compiler.calculations.CalculationUnit#toString()
	 */
	@Override
	public String toString() {
		return super.toString() + " - " + basicExpression.toString() ;
	}

	/**
	 * @see gumbo.compiler.calculations.CalculationUnit#getInputRelations()
	 */
	@Override
	public Set<RelationSchema> getInputRelations() {
		return basicExpression.getRelationDependencies();
	}

}