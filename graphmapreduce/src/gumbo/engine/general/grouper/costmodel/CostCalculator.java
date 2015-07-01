package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;

public interface CostCalculator {
	
	public double calculateCost(CalculationGroup semijoins);

}
