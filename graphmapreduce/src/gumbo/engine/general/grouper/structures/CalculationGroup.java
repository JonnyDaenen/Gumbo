package gumbo.engine.general.grouper.structures;

import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.util.List;
import java.util.Set;

public class CalculationGroup {
	
	Set<GuardedSemiJoinCalculation> semijoins;
	
	
	public void add(GuardedSemiJoinCalculation semijoin) {
		semijoins.add(semijoin);
	}
	
	public Set<GuardedSemiJoinCalculation> getAll() {
		return semijoins;
	}

	public void addAll(CalculationGroup g) {
		semijoins.addAll(g.semijoins);
	}

	public int size() {
		return semijoins.size();
	}
	
	public List<GFAtomicExpression> getGuardDistinctList() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public List<GFAtomicExpression> getGuardList() {
		// TODO Auto-generated method stub
		return null;
	}

	public List<GFAtomicExpression> getGuardedDistinctList() {
		// TODO Auto-generated method stub
		return null;
	}

	public int getKeys(GFAtomicExpression guard) {
		// TODO Auto-generated method stub
		return 0;
	}

}
