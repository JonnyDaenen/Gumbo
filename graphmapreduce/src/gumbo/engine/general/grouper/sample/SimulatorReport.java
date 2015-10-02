package gumbo.engine.general.grouper.sample;

public class SimulatorReport {
	
	

	private long guardInBytes;
	private long guardedInBytes;
	private long guardOutBytes;
	private long guardedOutBytes;
	
	
	public SimulatorReport() {
		this(0,0,0,0);
	}

	public SimulatorReport(long guardInBytes, long guardedInBytes, long guardOutBytes, long guardedOutBytes) {
		super();
		this.guardInBytes = guardInBytes;
		this.guardedInBytes = guardedInBytes;
		this.guardOutBytes = guardOutBytes;
		this.guardedOutBytes = guardedOutBytes;
	}
	

	public long getGuardInBytes() {
		return guardInBytes;
	}

	public long getGuardedInBytes() {
		return guardedInBytes;
	}

	public long getGuardOutBytes() {
		return guardOutBytes;
	}

	public long getGuardedOutBytes() {
		return guardedOutBytes;
	}

	public void setGuardInBytes(long guardInBytes) {
		this.guardInBytes = guardInBytes;
	}

	public void setGuardedInBytes(long guardedInBytes) {
		this.guardedInBytes = guardedInBytes;
	}

	public void setGuardOutBytes(long guardOutBytes) {
		this.guardOutBytes = guardOutBytes;
	}

	public void setGuardedOutBytes(long guardedOutBytes) {
		this.guardedOutBytes = guardedOutBytes;
	}

	public void addGuardInBytes(long byteSize) {
		this.guardInBytes += byteSize;
	}
	
	public void addGuardedInBytes(long byteSize) {
		this.guardedInBytes += byteSize;
	}
	
	public void addGuardOutBytes(long byteSize) {
		this.guardOutBytes += byteSize;
	}
	
	public void addGuardedOutBytes(long byteSize) {
		this.guardedOutBytes += byteSize;
	}

}
