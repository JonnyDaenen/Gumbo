package guardedfragment.structure.gfexpressions;

import guardedfragment.structure.booleanexpressions.BAndExpression;
import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;

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
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}



}
