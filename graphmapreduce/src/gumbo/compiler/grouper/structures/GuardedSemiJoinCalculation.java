package gumbo.compiler.grouper.structures;

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
	
	
	@Override
	public boolean equals(Object obj) {
		
		if (! (obj instanceof GuardedSemiJoinCalculation)){
			return false;
		}
		GuardedSemiJoinCalculation other = (GuardedSemiJoinCalculation) obj;
		
		return guard.equals(other.guard) && guarded.equals(other.guarded);
	}
	
	public int hashCode() {
		return guard.hashCode() ^ guarded.hashCode();
	};
}
