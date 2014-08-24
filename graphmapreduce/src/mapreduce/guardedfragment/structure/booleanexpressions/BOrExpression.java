package mapreduce.guardedfragment.structure.booleanexpressions;


public class BOrExpression extends BExpression{

	BExpression child1;
	BExpression child2;
	
	/**
	 * An Or-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public BOrExpression(BExpression c1, BExpression c2) {
		child1 = c1;
		child2 = c2;
	}

	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating both children and combining the result.
	 * 
	 * @return true iff one of the children evaluate to true
	 */
	public boolean evaluate(BEvaluationContext c) throws VariableNotFoundException {
		return child1.evaluate(c) || child2.evaluate(c);
	}


	@Override
	public String generateString() {
		return "(" + child1.generateString() + " | " + child2.generateString() + ")";
	}
	


}
