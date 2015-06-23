package gumbo.compiler.grouper.structures;

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

}
