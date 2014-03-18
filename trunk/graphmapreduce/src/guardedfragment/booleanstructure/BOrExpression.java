package guardedfragment.booleanstructure;

import java.util.Set;

public class BOrExpression extends BAndExpression{

	
	/**
	 * An OR-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public BOrExpression(BExpression c1, BExpression c2) {
		super(c1,c2);
		operatorSymbol = '|';
	}
	
	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating both children and combining the result.
	 * 
	 * @return true iff at least one child evaluates to true
	 */
	public boolean evaluate(BEvaluationContext c) throws VariableNotFoundException {

		return child1.evaluate(c) || child2.evaluate(c);
	}
	


}
