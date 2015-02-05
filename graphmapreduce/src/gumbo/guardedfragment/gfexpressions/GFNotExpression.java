package gumbo.guardedfragment.gfexpressions;


import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class GFNotExpression extends GFExpression{

	
	GFExpression child;
	int rank;
	
	/**
	 * An NOT-expression in the Guarded Fragment.
	 * @param c the child
	 */
	public GFNotExpression(GFExpression c) {
		child = c;
		rank = c.getRank();
	}
	

	
	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#addAtomic(java.util.Collection)
	 */
	@Override
	public void addAtomic(Collection<GFAtomicExpression> current) {
		child.addAtomic(current);
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
	public int getRank() {
		return this.rank;
	}
	
	/**
	 * @return the argument
	 */
	public GFExpression getChild() {
		return child;
	}


	@Override
	public Set<GFExistentialExpression> getSubExistentialExpression(int k) {
		Set<GFExistentialExpression> set = new HashSet<GFExistentialExpression>();
		
		if (k > this.rank) {
			return set;
		}
		
		set.addAll(child.getSubExistentialExpression(k));
		return set;
	}
	
	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}
	
	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return child.containsAnd();
	}





	/**
	 * Checks DNF, nested NOT is not allowed.
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#isInDNF()
	 */
	@Override
	public boolean isInDNF() {
		// check direct child, do not allow
		if( !(child instanceof GFAtomicExpression)  )
			return false;

		// recursive check child
		return child.isInDNF();
		
	}





	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#containsOr()
	 */
	@Override
	public boolean containsOr() {
		return child.containsOr();
	}





	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#countOccurences(gumbo.guardedfragment.gfexpressions.GFExpression)
	 */
	@Override
	public int countOccurences(GFExpression ge) {
		int thisok = 0;
		if(this == ge) 
			thisok = 1;
		
		return thisok + child.countOccurences(ge);
	}





	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFExpression#getParent(gumbo.guardedfragment.gfexpressions.GFExpression)
	 */
	@Override
	public GFExpression getParent(GFExpression e) {
		if(child == e )
			return this;
		
		GFExpression child1result = child.getParent(e);
		if(child1result != null)
			return child1result;
		
		return null;
	}





	
}
