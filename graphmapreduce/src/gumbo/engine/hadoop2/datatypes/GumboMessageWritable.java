package gumbo.engine.hadoop2.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class GumboMessageWritable implements WritableComparable<GumboMessageWritable> {

	private ByteWritable type;
	private VLongWritable fileid;
	private VLongWritable offset;
	private BytesWritable atomids;


	public GumboMessageWritable() {
		set(new ByteWritable(), new VLongWritable(), new VLongWritable(), new BytesWritable());
	}

	public GumboMessageWritable(byte type, long fileid, long offset, byte [] atomids) {

		set(new ByteWritable(type),
				new VLongWritable(fileid),
				new VLongWritable(offset),
				new BytesWritable(atomids));
	}

	public void set(ByteWritable type, VLongWritable fileid, VLongWritable offset, BytesWritable atomids) {
		this.type = type;
		this.fileid = fileid;
		this.offset = offset;
		this.atomids = atomids;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		type.write(out);
		fileid.write(out);
		offset.write(out);
		atomids.write(out);

	}
	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		fileid.readFields(in);
		offset.readFields(in);
		atomids.readFields(in);
	}

	@Override
	public int compareTo(GumboMessageWritable mw) {
		int cmp = type.compareTo(mw.type);

		if (cmp != 0){
			return cmp;
		}

		cmp = fileid.compareTo(mw.fileid);
		if (cmp != 0) {
			return cmp;
		}

		cmp = offset.compareTo(mw.offset);
		if (cmp != 0) {
			return cmp;
		}


		return atomids.compareTo(mw.atomids);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GumboMessageWritable) {
			GumboMessageWritable m2 = (GumboMessageWritable) obj;
			return type.equals(m2.type) && offset.equals(m2.offset)
					&& fileid.equals(m2.fileid) && atomids.equals(m2.atomids);

		}
		return false;
	}

	@Override
	public int hashCode() {
		return type.hashCode() ^ fileid.hashCode() ^ offset.hashCode() ^ atomids.hashCode();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(100);

		sb.append(type.get());
		sb.append(":");
		sb.append(fileid);
		sb.append(":");
		sb.append(offset);
		sb.append(":");
		byte[] bytes = atomids.getBytes();
		for (int i = 0; i < atomids.getLength(); i++) {
			sb.append(bytes[i]);
			sb.append(",");
		}

		return sb.toString();
	}


	public class GumboMessageType {
		public static final byte ASSERT = 1;
		public static final byte REQUEST = 2;
		public static final byte CONFIRM = 3;
		public static final byte DATA = 4;
	}


	public static class Comparator extends WritableComparator {
		private static final BytesWritable.Comparator BYTES_COMPARATOR = new BytesWritable.Comparator();

		public Comparator() { 
			super(GumboMessageWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				
				if (true)
					return BYTES_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);

				// compare message types
				if (b1[0] < b2[0])
					return -1;
				else if (b1[0] != b2[0])
					return 1;

				int offset1 = s1+1;
				int offset2 = s2+1;

				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
				long fileid1 = readVLong(b1, s1+1);

				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
				long fileid2 = readVLong(b2, s2+1);

				if (fileid1 != fileid2) {
					if (fileid1 < fileid2)
						return -1;
					else
						return 1;
				}

				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
				long fileoffset1 = readVLong(b1, s1+1);

				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
				long fileoffset2 = readVLong(b2, s2+1);

				if (fileoffset1 != fileoffset2) {
					if (fileoffset1 < fileoffset2)
						return -1;
					else
						return 1;
				}

				return BYTES_COMPARATOR.compare(b1, offset1, l1 + offset1 - s1 , b2, offset2, l2 + offset2 - s2);


			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			} 
		}
	}
	static {
		WritableComparator.define(GumboMessageWritable.class, new Comparator());
	}
	public void set(byte type2, long fileid2, long offset2, byte[] atomids1) {
		type.set(type2);
		fileid.set(fileid2);
		offset.set(offset2);
		atomids.set(atomids1, 0, atomids1.length);
	}

	public long getFileId() {
		return fileid.get();
	}
	
	public long getOffset() {
		return offset.get();
	}
	
	public boolean containsAtomId(byte needle) {
		
		byte[] atoms = atomids.getBytes();
		for (int i = 0; i < atomids.getLength(); i++) {
			if (atoms[i] == needle)
				return true;
		}
		return false;
	}

	public boolean isAssert() {
		return type.get() == GumboMessageType.ASSERT;
	}

	public void setContent(byte[] bytes, int length) {
		// TODO implement		
	}

	public boolean isData() {
		return type.get() == GumboMessageType.DATA;
	}

	public BytesWritable getAtomIDBytes() {
		return atomids;
	}

	public BytesWritable getContent() {
		// TODO implement
		return null;
	}


}
