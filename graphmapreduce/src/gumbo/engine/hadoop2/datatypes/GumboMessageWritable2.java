package gumbo.engine.hadoop2.datatypes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class GumboMessageWritable2 implements WritableComparable<GumboMessageWritable2> {


	private byte [] bytes;
	private int length;

	private byte type;
	private long fileid;
	private long offset;
	private byte [] atombytes;
	private boolean extracted;


	public GumboMessageWritable2() {
		extracted = false;
	}

	public GumboMessageWritable2(byte type, long fileid, long offset, byte [] atomids) {

		set(type, fileid, offset, atomids);
	}

	public void set(byte type, long fileid, long offset, byte [] atomids) {
		this.type = type;
		this.fileid = fileid;
		this.offset = offset;
		this.atombytes = atomids;
		extracted = true;

		compact();

	}

	private void compact() {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);

		try {
			dos.write(type);
			WritableUtils.writeVLong(dos, fileid);
			WritableUtils.writeVLong(dos, offset);
			WritableUtils.writeCompressedByteArray(dos, atombytes);
			
			length = bos.size();
			setCapacity(length);
			bytes = bos.toByteArray();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, length);
		out.write(bytes, 0, length);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int newLength = WritableUtils.readVInt(in);
		setCapacity(newLength);
		in.readFully(bytes, 0, newLength);
		length = newLength;
		extracted = false;
	}

	private void setCapacity(int len) {
		if (bytes == null || bytes.length < len) {
			bytes = new byte[len];
		}
	}

	@Override
	public int compareTo(GumboMessageWritable2 mw) {



		int cmp = type - mw.type;

		if (cmp != 0){
			return cmp;
		}

		cmp = (int)fileid - (int)mw.fileid;
		if (cmp != 0) {
			return cmp;
		}


		return (int)offset - (int)mw.offset;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GumboMessageWritable2) {
			GumboMessageWritable2 m2 = (GumboMessageWritable2) obj;
			extract();
			m2.extract();
			return type == m2.type && offset == m2.offset
					&& fileid == m2.fileid; // FIXME && atomids == m2.atomids;

		}
		return false;
	}

	@Override
	public int hashCode() {
		return (int) (fileid ^ offset) ;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(100);

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
			super(GumboMessageWritable2.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return BYTES_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	static {
		WritableComparator.define(GumboMessageWritable2.class, new Comparator());
	}

	public long getFileId() {
		extract();
		return fileid;
	}

	public long getOffset() {
		extract();
		return offset;
	}

	public byte getType() {
		extract();
		return type;
	}

	private void extract() {
		if (extracted || bytes == null)
			return;

		ByteArrayInputStream bis = new ByteArrayInputStream(bytes, 0, length);
		DataInputStream dis = new DataInputStream(bis);
//		System.out.println(length);
		try {
			type = (byte) dis.read();
			fileid = WritableUtils.readVLong(dis);
			offset = WritableUtils.readVLong(dis);
			atombytes = WritableUtils.readCompressedByteArray(dis);
			extracted = true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean containsAtomId(byte needle) {
		extract();

		for (int i = 0; i < atombytes.length; i++) {
			if (atombytes[i] == needle)
				return true;
		}
		return false;
	}

}
