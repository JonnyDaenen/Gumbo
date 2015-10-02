package gumbo.engine.general.grouper.policies;

import java.util.Collection;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * TODO implement
 * @author Jonny Daenen
 *
 */
public class CostMatrix {
	
	public CostMatrix() {
		
	}
	
	
	public void putCost(CalculationGroup job1, CalculationGroup job2, CalculationGroup cost) {
		
	}
	
	public double getCost(CalculationGroup job1, CalculationGroup job2) {
		return 0;
	}
	
	public void removeEntry(CalculationGroup job1, CalculationGroup job2) {
		
	}
	
	public void remove(CalculationGroup job) {
		
	}
	
	public void add(CalculationGroup job) {
		
	}
	
	
	public boolean hasPositiveCost() {
		return true;
	}
	
	public Pair<CalculationGroup,CalculationGroup> getBestOldGroups() {
		return null;
	}
	
	public CalculationGroup getBestNewGroup() {
		return null;
	}
	
	public Collection<CalculationGroup> getGroups() {
		return null;
	}

}
