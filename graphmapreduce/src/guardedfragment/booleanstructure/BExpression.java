package guardedfragment.booleanstructure;


public interface BExpression {
	
	boolean evaluate(BEvaluationContext c) throws VariableNotFoundException;
	
//	Set<String> getFreeVariables();
	
	
	String generateString();
	
//	String generateString(BEvaluationContext c);
	
}
