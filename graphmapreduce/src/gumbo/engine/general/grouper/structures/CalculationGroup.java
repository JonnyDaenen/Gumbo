package gumbo.engine.general.grouper.structures;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CalculationGroup {

	Set<GuardedSemiJoinCalculation> semijoins;
	double cost;

	public double getCost() {
		return cost;
	}


	public void setCost(double cost) {
		this.cost = cost;
	}


	public CalculationGroup() {
		semijoins = new HashSet<>();
	}


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

	public Collection<GFAtomicExpression> getGuardsDistinct() {
		Set<GFAtomicExpression> result = new HashSet<>();
		for (GuardedSemiJoinCalculation sj : semijoins) {
			result.add(sj.getGuard());
		}
		return result;
	}

	public Collection<GFAtomicExpression> getGuardedsDistinct() {
		Set<GFAtomicExpression> result = new HashSet<>();
		for (GuardedSemiJoinCalculation sj : semijoins) {
			result.add(sj.getGuarded());
		}
		return result;
	}


	public Collection<RelationSchema> getAllSchemas() {
		Set<RelationSchema> result = new HashSet<>();
		for (GuardedSemiJoinCalculation sj : semijoins) {
			result.add(sj.getGuard().getRelationSchema());
			result.add(sj.getGuarded().getRelationSchema());
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (GuardedSemiJoinCalculation sj : semijoins) {
			sb.append("\t" + sj.toString());
			sb.append(System.lineSeparator());
		}

		return sb.toString();

	}

	@Override
	public int hashCode() {
		int hash = 0;
		for (GuardedSemiJoinCalculation sj : semijoins) {
			hash ^= sj.hashCode();
		}
		return hash;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CalculationGroup) {
			CalculationGroup group = (CalculationGroup) obj;
			return semijoins.equals(group.semijoins);
		}
		return false;
	}

}
