package guardedfragment.booleanstructure;

public class BAndExpression implements BExpression{
	
	
	BExpression child1;
	BExpression child2;
	
	char operatorSymbol;
	
	/**
	 * An AND-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public BAndExpression(BExpression c1, BExpression c2) {
		child1 = c1;
		child2 = c2;
		
		operatorSymbol = '&';
	}

	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating both children and combining the result.
	 * 
	 * @return true iff both children evaluate to true
	 */
	public boolean evaluate(BEvaluationContext c) throws VariableNotFoundException {
		return child1.evaluate(c) && child2.evaluate(c);
	}


	@Override
	public String generateString() {
		return "(" + child1.generateString() + " " + operatorSymbol + " " + child2.generateString() + ")";
	}


//	@Override
//	public Set<String> getFreeVariables() {
//		Set<String> freeVars = child1.getFreeVariables();
//		freeVars.addAll(child2.getFreeVariables());
//		
//		return freeVars;
//	}

}
