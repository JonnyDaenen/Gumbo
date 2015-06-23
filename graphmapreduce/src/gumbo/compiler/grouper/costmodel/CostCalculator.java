package gumbo.compiler.grouper.costmodel;

import gumbo.compiler.grouper.structures.CalculationGroup;

import java.util.Set;

public interface CostCalculator {
	
	public double calculateCost(CalculationGroup semijoins);

}
