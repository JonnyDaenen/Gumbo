package guardedfragment.booleanstructure;

import guardedfragment.structure.GFEvaluationContext;

import java.util.Set;

public interface BExpression {
	
	boolean evaluate(BEvaluationContext c) throws VariableNotFoundException;
	
//	Set<String> getFreeVariables();
	
	
	String generateString();
	
//	String generateString(BEvaluationContext c);
	
}
