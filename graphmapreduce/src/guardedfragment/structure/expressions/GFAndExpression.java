package guardedfragment.structure.expressions;

import guardedfragment.booleanstructure.BAndExpression;
import guardedfragment.booleanstructure.BExpression;
import guardedfragment.structure.GFBMapping;
import guardedfragment.structure.GFConversionException;
import guardedfragment.structure.GFEvaluationContext;

import java.util.HashSet;
import java.util.Set;

public class GFAndExpression extends GFExpression{
	
	
	GFExpression child1;
	GFExpression child2;
	int rank;
		
	/**
	 * An AND-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public GFAndExpression(GFExpression c1, GFExpression c2) {
		child1 = c1;
		child2 = c2;
		rank = Math.max(child1.getRank(),child2.getRank());
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

	public String prefixString() {
		return "&" + child1.prefixString() + child2.prefixString();
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


	@Override
	public int getRank() {
		return this.rank;
	}


	@Override
	public Set<GFExistentialExpression> getSubExistentialExpression(int k) {

		Set<GFExistentialExpression> set = new HashSet<GFExistentialExpression>();
		
		if (k > this.rank) {
			return set;
		}
		
		set.addAll(child1.getSubExistentialExpression(k));
		set.addAll(child2.getSubExistentialExpression(k));
		return set;
	}
	
	
	/**
	 * @return left argument
	 */
	public GFExpression getChild1() {
		return child1;
	}
	
	/**
	 * @return right argument
	 */
	public GFExpression getChild2() {
		return child2;
	}


	@Override
	public <R> R accept(GFVisitor<R> v) {
		return v.visit(this);
	}



}
