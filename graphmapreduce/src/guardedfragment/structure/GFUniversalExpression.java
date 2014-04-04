package guardedfragment.structure;

public class GFUniversalExpression extends GFExistentialExpression {

	/**
	 * Creates a universal expression, consisting of a guard (atomic expression) 
	 * and a child expression.
	 * @param guard an atomic expression
	 * @param child a child expression
	 * @param variables the quantified variables
	 */
	public GFUniversalExpression(GFAtomicExpression guard, GFExpression child, String name, String[] variables) {
		super(guard, child, name, variables);
		super.quantifierSymbol = 'A';
	}

	@Override
	/**
	 * Checks if the formula evaluates to true for all assignments 
	 * (of values in the domain) to the quantified variables.
	 * 
	 * @return true iff the formula evaluates to true for all values
	 */
	public boolean evaluate(GFEvaluationContext c) {
		
		// TODO Auto-generated method stub
		
		// evaluate the implication
		return !guard.evaluate(c) || child.evaluate(c);
	}

}
