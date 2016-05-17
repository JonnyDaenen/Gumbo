package gumbo.engine.hadoop2.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class VLongPair implements WritableComparable<VLongPair> {

	private VLongWritable l1;
	private VLongWritable l2;
	
	private long first;
	private long second;


	public VLongPair() {
	}

	public VLongPair(long l1, long l2) {
		first = l1;
		second = l2;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeVLong(out, first);
		WritableUtils.writeVLong(out, second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = WritableUtils.readVLong(in);
		second = WritableUtils.readVLong(in);
	}

	@Override
	public int compareTo(VLongPair mw) {
		int cmp = l1.compareTo(mw.l1);

		if (cmp != 0){
			return cmp;
		}

		return l2.compareTo(mw.l2);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof VLongPair) {
			VLongPair m2 = (VLongPair) obj;
			return l1.equals(m2.l1) && l2.equals(m2.l2);

		}
		return false;
	}

	@Override
	public int hashCode() {
		return l1.hashCode() ^ l2.hashCode();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(100);

		sb.append(l1.get());
		sb.append(":");
		sb.append(l2.get());

		return sb.toString();
	}



	public static class Comparator extends WritableComparator {

		RawComparator<Text> RAW_COMPARATOR =
				WritableComparator.get(VLongWritable.class);

		private static final BytesWritable.Comparator BYTES_COMPARATOR = new BytesWritable.Comparator();

		public Comparator() { 
			super(VLongPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			return BYTES_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
			
//			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]); 
//			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]); 
//
//			int cmp = RAW_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2); 
//			if (cmp!=0) {
//				return cmp; 
//			}
//			return RAW_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);

		}
	}
	static {
		WritableComparator.define(VLongPair.class, new Comparator());
	}

	public void set(long l1, long l2) {
		this.l1.set(l1);
		this.l2.set(l2);
	}

	public long getFirst() {
		return first;
	}

	public long getSecond() {
		return second;
	}

	public void setFirst(long l1) {
		this.l1.set(l1);
	}
	
	public void setSecond(long l2) {
		this.l2.set(l2);
	}
	
	public void setFirst(VLongWritable l) {
		l1 = l;
	}
	
	public void setSecond(VLongWritable l) {
		l2 = l;
	}



}
