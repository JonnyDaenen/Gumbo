package gumbo.engine.hadoop.reporter;

import java.lang.reflect.Field;

import gumbo.structures.data.RelationSchema;

public class RelationReport {

	protected long numFiles = 0;

	public long estInputTuples = 0;
	protected long numInputBytes = 0;

	public long estIntermTuples = 0;
	public long estIntermBytes = 0;
	public long estIntermKeyBytes = 0;
	public long estIntermValueBytes = 0;


	protected RelationSchema rs;
	//	protected Set<Path> files;
	//	protected Set<?> atoms;

	public RelationReport(RelationSchema rs) {
		this.rs = rs;
	}


	public long getNumFiles() {
		return numFiles;
	}
	public void setNumFiles(long numFiles) {
		this.numFiles = numFiles;
	}
	public long getEstInputTuples() {
		return estInputTuples;
	}
	public void setEstInputTuples(long estInputTuples) {
		this.estInputTuples = estInputTuples;
	}
	public long getNumInputBytes() {
		return numInputBytes;
	}
	public void setNumInputBytes(long numInputBytes) {
		this.numInputBytes = numInputBytes;
	}
	public long getEstIntermTuples() {
		return estIntermTuples;
	}
	public void setEstIntermTuples(long estIntermTuples) {
		this.estIntermTuples = estIntermTuples;
	}
	public long getEstIntermBytes() {
		return estIntermBytes;
	}
	public void setEstIntermBytes(long estIntermBytes) {
		this.estIntermBytes = estIntermBytes;
	}
	public long getEstIntermKeyBytes() {
		return estIntermKeyBytes;
	}
	public void setEstIntermKeyBytes(long estIntermKeyBytes) {
		this.estIntermKeyBytes = estIntermKeyBytes;
	}
	public long getEstIntermValueBytes() {
		return estIntermValueBytes;
	}
	public void setEstIntermValueBytes(long estIntermValueBytes) {
		this.estIntermValueBytes = estIntermValueBytes;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		try {
			sb.append("Relation: " + rs);
			for (Field f: this.getClass().getDeclaredFields()) {
				sb.append(System.lineSeparator());
				sb.append(f.getName() +": "+ f.get(this));

			}
		} catch (IllegalAccessException | IllegalArgumentException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}
}
