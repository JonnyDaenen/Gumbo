package gumbo.engine.general.grouper.structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;


/**
 * Representation of binary guard-guarded atom semijoin.
 * 
 * @author Jonny Daenen
 *
 */
public class GFCalculation extends GuardedSemiJoinCalculation {

	GFExistentialExpression e;

	public GFCalculation(GFExistentialExpression e2) {
		super(null,null);
		this.e = e2;
	}
	
	public GFAtomicExpression getGuard() {
		return e.getGuard();
	}
	
	public List<GFAtomicExpression> getGuarded() {
		ArrayList<GFAtomicExpression> list = new ArrayList<GFAtomicExpression>(e.getGuardedAtoms());
		Collections.sort(list);
		
		return list;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		
		if (! (obj instanceof GFCalculation)){
			return false;
		}
		GFCalculation other = (GFCalculation) obj;
		
		return e.equals(other.e);
	}
	
	public int hashCode() {
		return e.hashCode();
	}

	@Override
	public String toString() {
		return e.toString();
	}
	
	public GFExistentialExpression getExpression() {
		return e;
	}

	public String getCanonicalString() {
		return e.toString();
	};
	
	
}
