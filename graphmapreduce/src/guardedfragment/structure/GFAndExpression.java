package guardedfragment.structure;

import guardedfragment.booleanstructure.BAndExpression;
import guardedfragment.booleanstructure.BExpression;

import java.util.Set;

public class GFAndExpression extends GFExpression{
	
	
	GFExpression child1;
	GFExpression child2;
		
	/**
	 * An AND-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public GFAndExpression(GFExpression c1, GFExpression c2) {
		child1 = c1;
		child2 = c2;
	}

	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating both children and combining the result.
	 * 
	 * @return true iff both children evaluate to true
	 */
	public boolean evaluate(GFEvaluationContext c) {
		return child1.evaluate(c) && child2.evaluate(c);
	}


	@Override
	public Set<String> getFreeVariables() {
		Set<String> freeVars = child1.getFreeVariables();
		freeVars.addAll(child2.getFreeVariables());
		
		return freeVars;
	}

	public Set<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> allAtoms = child1.getAtomic();
		allAtoms.addAll(child2.getAtomic());
		
		return allAtoms;
	}

	@Override
	public String generateString() {
		return "(" + child1.generateString() + " & " + child2.generateString() + ")";
	}


	@Override
	public boolean isGuarded() {
		return child1.isGuarded() && child2.isGuarded();
	}
	
	@Override
	public boolean isAtomicBooleanCombination() {
		return child1.isAtomicBooleanCombination() && child2.isAtomicBooleanCombination();
	}


	@Override
	public BExpression convertToBExpression(GFBMapping m) throws GFConversionException {
		BExpression nc1 = child1.convertToBExpression(m);
		BExpression nc2 = child2.convertToBExpression(m);
		return new BAndExpression(nc1, nc2);
	}

}
