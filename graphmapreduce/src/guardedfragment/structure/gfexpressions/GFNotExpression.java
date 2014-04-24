package guardedfragment.structure.gfexpressions;


import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.BNotExpression;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;

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
	
	

	
	
	public Set<GFAtomicExpression> getAtomic() {
		Set<GFAtomicExpression> allAtoms = child.getAtomic();
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
	
	public String prefixString() {
		return "!" + child.prefixString();
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
	public BExpression convertToBExpression(GFBooleanMapping m) throws GFtoBooleanConversionException {
		BExpression c = child.convertToBExpression(m);
		return new BNotExpression(c);
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
	public <R> R accept(GFVisitor<R> v) {
		return v.visit(this);
	}
}
