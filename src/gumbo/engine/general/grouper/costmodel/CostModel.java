package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;

public interface CostModel {
	
	public double calculateCost(CalculationGroup job);

	public double calculateCost(SimulatorReport report);
}
