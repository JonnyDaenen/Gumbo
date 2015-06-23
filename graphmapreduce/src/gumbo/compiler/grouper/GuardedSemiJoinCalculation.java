package gumbo.compiler.grouper;

import gumbo.structures.gfexpressions.GFAtomicExpression;


/**
 * Representation of binary guard-guarded atom semijoin.
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedSemiJoinCalculation {

	private GFAtomicExpression guard;
	private GFAtomicExpression guarded;

	public GuardedSemiJoinCalculation(GFAtomicExpression guard,
			GFAtomicExpression guarded) {
		this.guard = guard;
		this.guarded = guarded;
	}
	
	public GFAtomicExpression getGuard() {
		return guard;
	}
	
	public GFAtomicExpression getGuarded() {
		return guarded;
	}
	
}
