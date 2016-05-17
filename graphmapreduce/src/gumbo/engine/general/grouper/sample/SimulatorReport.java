package gumbo.engine.general.grouper.sample;

import java.util.LinkedList;
import java.util.List;

public class SimulatorReport {



	private long guardInBytes;
	private long guardedInBytes;
	private long guardOutBytes;
	private long guardedOutBytes;

	boolean hasDetails = false;

	private List<RelationMapReport> details;

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

		this.details = new LinkedList<>();
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

	public void addGuardDetails(long in, long out, long inrec, long outrec) {
		addGuardInBytes(in);
		addGuardOutBytes(out);

		RelationMapReport rep = new RelationMapReport();
		rep.guardInBytes = in*scale;
		rep.guardOutBytes = out*scale;
		rep.guardInRecords = inrec*scale;
		rep.guardOutRecords = outrec*scale;

		details.add(rep);
		hasDetails = true;

	}

	public void addGuardedDetails(long in, long out, long inrec, long outrec) {
		addGuardedInBytes(in);
		addGuardedOutBytes(out);

		RelationMapReport rep = new RelationMapReport();
		rep.guardedInBytes = in*scale;
		rep.guardedOutBytes = out*scale;
		rep.guardedInRecords = inrec*scale;
		rep.guardedOutRecords = outrec*scale;

		details.add(rep);
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
		for (RelationMapReport p : details) {
			s += p.toString();
		}
		return s;
	}

	public List<RelationMapReport> getDetails() {
		return details;
	}


	public class RelationMapReport {
		public long guardInBytes = 0;
		public long guardOutBytes = 0;
		public long guardInRecords = 0;
		public long guardOutRecords = 0;

		public long guardedInBytes = 0;
		public long guardedOutBytes = 0;
		public long guardedInRecords = 0;
		public long guardedOutRecords = 0;

		@Override
		public String toString() {
			String s = "";
			if (guardInBytes != 0)
				s += "\t " + guardInBytes + " -> " + guardOutBytes + " (G-B)\n";
			if (guardedInBytes != 0)
				s += "\t " + guardedInBytes + " -> " + guardedOutBytes + " (g-B)\n";
			if (guardInRecords != 0)
				s += "\t " + guardInRecords + " -> " + guardOutRecords + " (G-R)\n";
			if (guardedInRecords != 0)
				s += "\t " + guardedInRecords + " -> " + guardedOutRecords + " (g-R)\n";
			return s;

		}
		
		public long getInBytes() {
			return guardInBytes + guardedInBytes;
		}
		
		public long getOutBytes() {
			return guardOutBytes + guardedOutBytes;
		}

		public long getInRecords() {
			return guardInRecords + guardedInRecords;
		}
		
		public long getOutRecords() {
			return guardOutRecords + guardedOutRecords;
		}
	}


	public long getTotalMapOutRec() {
		long sum = 0;
		for (RelationMapReport p : details) {
			sum += p.guardOutRecords + p.guardedOutRecords;
		}
		return sum;
	}

}
