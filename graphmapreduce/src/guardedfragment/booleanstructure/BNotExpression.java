package guardedfragment.booleanstructure;


public class BNotExpression extends BExpression{

	
	BExpression child;
	
	/**
	 * An NOT-expression in the Guarded Fragment.
	 * @param c the child
	 */
	public BNotExpression(BExpression c) {
		child = c;
	}
	
	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating the child and negating the result.	 
	 * 
	 * @return true iff the child returns false
	 */
	public boolean evaluate(BEvaluationContext c) throws VariableNotFoundException {

		return !child.evaluate(c) ;
	}
	
	public String generateString() {
		return "(!" + child.generateString() + ")";
	}
	
//	@Override
//	public Set<String> getFreeVariables() {
//		Set<String> freeVars = child.getFreeVariables();
//		return freeVars;
//	}
}
