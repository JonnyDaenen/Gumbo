package gumbo.engine.general.grouper.sample;

import java.util.LinkedList;
import java.util.List;

import gumbo.structures.gfexpressions.io.Pair;

public class SimulatorReport {
	
	

	private long guardInBytes;
	private long guardedInBytes;
	private long guardOutBytes;
	private long guardedOutBytes;
	
	boolean hasDetails = false;
	
	private List<Pair<Long,Long>> detailGuard;
	private List<Pair<Long,Long>> detailGuarded;
	
	private long scale = 1;//FIXME 1500; // remove!!
	
	
	public SimulatorReport() {
		this(0,0,0,0);
	}

	public SimulatorReport(long guardInBytes, long guardedInBytes, long guardOutBytes, long guardedOutBytes) {
		super();
		this.guardInBytes = guardInBytes;
		this.guardedInBytes = guardedInBytes;
		this.guardOutBytes = guardOutBytes;
		this.guardedOutBytes = guardedOutBytes;

		this.detailGuard = new LinkedList<>();
		this.detailGuarded = new LinkedList<>();
	}
	
	
	

	public long getGuardInBytes() {
		return guardInBytes;
	}

	public long getGuardedInBytes() {
		return guardedInBytes ;
	}

	public long getGuardOutBytes() {
		return guardOutBytes ;
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
		this.guardInBytes += byteSize * scale;
	}
	
	public void addGuardedInBytes(long byteSize) {
		this.guardedInBytes += byteSize * scale;
	}
	
	public void addGuardOutBytes(long byteSize) {
		this.guardOutBytes += byteSize * scale;
	}
	
	public void addGuardedOutBytes(long byteSize) {
		this.guardedOutBytes += byteSize * scale;
	}
	
	public void addGuardDetails(long in, long out) {
		addGuardInBytes(in);
		addGuardOutBytes(out);
		detailGuard.add(new Pair<>(in*scale,out*scale));
		hasDetails = true;
		
	}
	
	public void addGuardedDetails(long in, long out) {
		addGuardedInBytes(in);
		addGuardedOutBytes(out);
		detailGuarded.add(new Pair<>(in*scale,out*scale));
		hasDetails = true;
		
	}
	
	public boolean hasDetails() {
		return hasDetails;
	}
	
	@Override
	public String toString() {
		String s = "";
		s += "Guard in bytes: " + guardInBytes + "\n";
		s += "Guarded in bytes: " + guardedInBytes + "\n";
		s += "Guard out bytes: " + guardOutBytes + "\n";
		s += "Guarded out bytes: " + guardedOutBytes + "\n";
		s += "Details:\n";
		for (Pair<Long, Long> p : detailGuard) {
			s += "\t " + p.fst + " -> " + p.snd + "\n";
		}
		for (Pair<Long, Long> p : detailGuarded) {
			s += "\t " + p.fst + " -> " + p.snd + "\n";
		}
		return s;
	}

	public List<Pair<Long,Long>> getGuardDetails() {
		return detailGuard;
	}
	
	public List<Pair<Long,Long>> getGuardedDetails() {
		return detailGuarded;
	}

}
