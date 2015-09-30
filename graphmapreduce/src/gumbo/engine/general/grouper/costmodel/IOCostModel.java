package gumbo.engine.general.grouper.costmodel;

public class IOCostModel implements CostModel {

	@Override
	public double calculateCost(MRSettings s, GroupedJob job) {
		
		return job.getGuardedInBytes() + job.getGuardInBytes() + job.getGuardedOutBytes() + job.getGuardOutBytes();
	}
	


	

}
