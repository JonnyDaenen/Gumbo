package gumbo.engine.general.grouper.structures;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CalculationGroup {
	
	long scale = 1; // FIXME this should remain 1 in release versions!

	Set<GuardedSemiJoinCalculation> semijoins;
	
	long guardInBytes;
	long guardedInBytes;
	long guardOutBytes;
	long guardedOutBytes;
	
	double cost;

	private Collection<GFExistentialExpression> sameLevelExpressions;


	
	public CalculationGroup(Collection<GFExistentialExpression> relevantExpressions) {
		semijoins = new HashSet<>();
		this.sameLevelExpressions = relevantExpressions;
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

		sb.append("\tGuard In Bytes" + guardInBytes + System.lineSeparator());
		sb.append("\tGuarded In Bytes" + guardedInBytes + System.lineSeparator());
		sb.append("\tGuard Out Bytes" + guardOutBytes + System.lineSeparator());
		sb.append("\tGuarded Out Bytes" + guardedOutBytes + System.lineSeparator());
		sb.append("\tCost:" + cost + System.lineSeparator());
		

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
	
	public double getCost() {
		return cost;
	}


	public void setCost(double cost) {
		this.cost = cost;
	}


	public long getGuardInBytes() {
		return guardInBytes * scale;
	}


	public void setGuardInBytes(long guardInBytes) {
		this.guardInBytes = guardInBytes;
	}


	public long getGuardedInBytes() {
		return guardedInBytes * scale;
	}


	public void setGuardedInBytes(long guardedInBytes) {
		this.guardedInBytes = guardedInBytes;
	}


	public long getGuardOutBytes() {
		return guardOutBytes * scale;
	}


	public void setGuardOutBytes(long guardOutBytes) {
		this.guardOutBytes = guardOutBytes;
	}


	public long getGuardedOutBytes() {
		return guardedOutBytes * scale;
	}


	public void setGuardedOutBytes(long guardedOutBytes) {
		this.guardedOutBytes = guardedOutBytes;
	}


	/**
	 * Creates a new group from two given groups.
	 * The resulting group contains a union of expressions,
	 * but all costs are 0.
	 * @param g the other group
	 * @return a representation of the merge result of the two groups
	 */
	public CalculationGroup merge(CalculationGroup g) {
		CalculationGroup result = new CalculationGroup(this.sameLevelExpressions);
		result.semijoins.addAll(this.semijoins);
		result.semijoins.addAll(g.semijoins);
		
		return result;
	}



	public Collection<RelationSchema> getInputRelations() {
		HashSet<RelationSchema> result = new HashSet<RelationSchema>(semijoins.size());
		
		for (GuardedSemiJoinCalculation sj : semijoins) {
			result.add(sj.getGuard().getRelationSchema());
			result.add(sj.getGuarded().getRelationSchema());
		}
		
		return result;
	}


	public Collection<GFExistentialExpression> getExpressions() {
		HashSet<GFExistentialExpression> result = new HashSet<GFExistentialExpression>(semijoins.size());
		
		for (GuardedSemiJoinCalculation sj : semijoins) {
			result.add(sj.getExpression());
		}
		
		return result;
	}


	public Collection<GFExistentialExpression> getRelevantExpressions() {
		if (sameLevelExpressions == null)
			return getExpressions();
		return sameLevelExpressions;
		
	}

}
