package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;
import guardedfragment.booleanstructure.BNotExpression;

import java.util.Set;

public class GFNotExpression implements GFExpression{

	
	GFExpression child;
	
	/**
	 * An NOT-expression in the Guarded Fragment.
	 * @param c the child
	 */
	public GFNotExpression(GFExpression c) {
		child = c;
	}
	
	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating the child and negating the result.	 
	 * 
	 * @return true iff the child returns false
	 */
	public boolean evaluate(GFEvaluationContext c) {

		return !child.evaluate(c) ;
	}
	
	
	public Set<GFExpression> getAtomic() {
		Set<GFExpression> allAtoms = child.getAtomic();
		return allAtoms;
	}
	
	@Override
	public Set<String> getFreeVariables() {
		Set<String> freeVars = child.getFreeVariables();
		return freeVars;
	}
	
	@Override
	public String generateString() {
		return "(!" + child.generateString() + ")";
	}
	
	
	@Override
	public boolean isGuarded() {
		return child.isGuarded();
	}


	@Override
	public boolean isAtomicBooleanCombination() {
		return child.isAtomicBooleanCombination();
	}


	@Override
	public BExpression convertToBExpression(GFBMapping m) throws GFConversionException {
		BExpression c = child.convertToBExpression(m);
		return new BNotExpression(c);
	}
}
