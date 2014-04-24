package guardedfragment.structure.booleanexpressions;


public abstract class BExpression {
	
	public abstract boolean evaluate(BEvaluationContext c) throws VariableNotFoundException;
	
//	Set<String> getFreeVariables();
	
	
	public abstract String generateString();
	
//	String generateString(BEvaluationContext c);
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.generateString();
	}
	
}
