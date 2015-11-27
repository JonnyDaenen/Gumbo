package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;

public class IOCostModel implements CostModel {

	@Override
	public double calculateCost(CalculationGroup job) {
		
		return job.getGuardedInBytes() + job.getGuardInBytes() + job.getGuardedOutBytes() + job.getGuardOutBytes();
	}
	


	

}
