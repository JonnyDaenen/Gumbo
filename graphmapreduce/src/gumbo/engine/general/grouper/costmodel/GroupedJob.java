package gumbo.engine.general.grouper.costmodel;

public class GroupedJob {
	
	
	// basic expressions
	
	long guardInBytes;
	long guardedInBytes;
	long guardOutBytes;
	long guardedOutBytes;
	
	double cost;
	
	

	public GroupedJob(long guardInBytes, long guardedInBytes, long guardOutBytes, long guardedOutBytes) {
		super();
		this.guardInBytes = guardInBytes;
		this.guardedInBytes = guardedInBytes;
		this.guardOutBytes = guardOutBytes;
		this.guardedOutBytes = guardedOutBytes;
		this.cost = 0;
	}

	public long getGuardInBytes() {
		return guardInBytes;
	}

	public void setGuardInBytes(long guardInBytes) {
		this.guardInBytes = guardInBytes;
	}

	public long getGuardedInBytes() {
		return guardedInBytes;
	}

	public void setGuardedInBytes(long guardedInBytes) {
		this.guardedInBytes = guardedInBytes;
	}

	public long getGuardOutBytes() {
		return guardOutBytes;
	}

	public void setGuardOutBytes(long guardOutBytes) {
		this.guardOutBytes = guardOutBytes;
	}

	public long getGuardedOutBytes() {
		return guardedOutBytes;
	}

	public void setGuardedOutBytes(long guardedOutBytes) {
		this.guardedOutBytes = guardedOutBytes;
	}

	public double getCost() {
		return cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}
	

}
