package gumbo.structures.gfexpressions;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;



public class GFOrExpression extends GFExpression{

	GFExpression child1,child2;
	int rank;

	/**
	 * An OR-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public GFOrExpression(GFExpression c1, GFExpression c2) {
		child1 = c1;
		child2 = c2;
		rank = Math.max(c1.getRank(),c2.getRank());
	}
	

	@Override
	public String generateString() {
		return "(" + child1.generateString() + " | " + child2.generateString() + ")";
	}
	

	

	
	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}
	
	@Override
	public Set<String> getFreeVariables() {
		Set<String> freeVars = child1.getFreeVariables();
		freeVars.addAll(child2.getFreeVariables());
		
		return freeVars;
	}
	
	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#addAtomic(java.util.Collection)
	 */
	@Override
	public void addAtomic(Collection<GFAtomicExpression> current) {
		child1.addAtomic(current);
		child2.addAtomic(current);
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




	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#isInDNF()
	 */
	@Override
	public boolean isInDNF() {
		return child1.isInDNF() && child2.isInDNF();
	}


	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return child1.containsAnd() || child2.containsAnd();
	}


	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#containsOr()
	 */
	@Override
	public boolean containsOr() {
		return true;
	}


	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#countOccurences(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public int countOccurences(GFExpression ge) {
		int thisok = 0;
		if(this == ge) 
			thisok = 1;
		
		return thisok + child1.countOccurences(ge) + child2.countOccurences(ge);
	}


	/**
	 * @see gumbo.structures.gfexpressions.GFExpression#getParent(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public GFExpression getParent(GFExpression e) {
		if(child1 == e || child2 == e)
			return this;
		
		GFExpression child1result = child1.getParent(e);
		if(child1result != null)
			return child1result;
		
		GFExpression child2result = child2.getParent(e);
		if(child2result != null)
			return child2result;
		
		return null;
	}

}
