package gumbo.engine.hadoop2.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;


public class GumboMessageWritable implements WritableComparable<GumboMessageWritable> {


	private static final Log LOG = LogFactory.getLog(GumboMessageWritable.class);
	
	public class GumboMessageType {
		public static final byte ASSERT = 1;
		public static final byte REQUEST = 2;
		public static final byte CONFIRM = 3;
		public static final byte DATA = 4;
	}

	

	private ByteWritable type;
	private VLongWritable fileid;
	private VLongWritable offset;
	private BytesWritable data;
	private DataOutputBuffer buffer;


	public GumboMessageWritable() {
		set(new ByteWritable(), new VLongWritable(), new VLongWritable(), new BytesWritable());
	}

	public GumboMessageWritable(byte type, long fileid, long offset, byte [] data, int length) {

		set(new ByteWritable(type),
				new VLongWritable(fileid),
				new VLongWritable(offset),
				new BytesWritable(data, length));
	}

	public void set(ByteWritable type, VLongWritable fileid, VLongWritable offset, BytesWritable data) {
		this.type = type;
		this.fileid = fileid;
		this.offset = offset;
		this.data = data;

	}

	@Override
	public void write(DataOutput out) throws IOException {

		switch (type.get()) {
		case GumboMessageType.REQUEST:
			type.write(out);
			fileid.write(out);
			offset.write(out);
			data.write(out);
			break;

		case GumboMessageType.ASSERT:
			type.write(out);
			data.write(out);
			break;

		case GumboMessageType.CONFIRM:
			type.write(out);
			data.write(out);
			break;

		case GumboMessageType.DATA:
			type.write(out);
			data.write(out);
		default:
			break;
		}

	}
	@Override
	public void readFields(DataInput in) throws IOException {

		type.readFields(in);
		switch (type.get()) {
		case GumboMessageType.REQUEST:
			fileid.readFields(in);
			offset.readFields(in);
			data.readFields(in);
			break;

		case GumboMessageType.ASSERT:
			data.readFields(in);
			break;

		case GumboMessageType.CONFIRM:
			data.readFields(in);
			break;

		case GumboMessageType.DATA:
			data.readFields(in);
		default:
			break;
		}


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


		return data.compareTo(mw.data);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof GumboMessageWritable) {
			GumboMessageWritable m2 = (GumboMessageWritable) obj;
			return type.equals(m2.type) && offset.equals(m2.offset)
					&& fileid.equals(m2.fileid) && data.equals(m2.data);

		}
		return false;
	}

	@Override
	public int hashCode() {
		return type.hashCode() ^ fileid.hashCode() ^ offset.hashCode() ^ data.hashCode();
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(100);

		sb.append(type.get());
		sb.append(":");
		if (type.get() == GumboMessageType.REQUEST) {
			sb.append(fileid);
			sb.append(":");
			sb.append(offset);
			sb.append(":");
		}
		byte[] bytes = data.getBytes();
		for (int i = 0; i < data.getLength(); i++) {
			sb.append(bytes[i]);
			sb.append(",");
		}

		return sb.toString();
	}


	

	public static class Comparator extends WritableComparator {
		private static final BytesWritable.Comparator BYTES_COMPARATOR = new BytesWritable.Comparator();

		public Comparator() { 
			super(GumboMessageWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return BYTES_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
//			try {
//				// compare message types
//				if (b1[0] < b2[0])
//					return -1;
//				else if (b1[0] != b2[0])
//					return 1;
//
//				int offset1 = s1+1;
//				int offset2 = s2+1;
//
//				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
//				long fileid1 = readVLong(b1, s1+1);
//
//				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
//				long fileid2 = readVLong(b2, s2+1);
//
//				if (fileid1 != fileid2) {
//					if (fileid1 < fileid2)
//						return -1;
//					else
//						return 1;
//				}
//
//				offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
//				long fileoffset1 = readVLong(b1, s1+1);
//
//				offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
//				long fileoffset2 = readVLong(b2, s2+1);
//
//				if (fileoffset1 != fileoffset2) {
//					if (fileoffset1 < fileoffset2)
//						return -1;
//					else
//						return 1;
//				}
//
//				return BYTES_COMPARATOR.compare(b1, offset1, l1 + offset1 - s1 , b2, offset2, l2 + offset2 - s2);
//
//
//			} catch (IOException e) {
//				throw new IllegalArgumentException(e);
//			} 
		}
	}
	static {
		WritableComparator.define(GumboMessageWritable.class, new Comparator());
	}
	public void set(byte type2, long fileid2, long offset2, byte[] data) {
		type.set(type2);
		fileid.set(fileid2);
		offset.set(offset2);
		this.data.set(data, 0, data.length);
	}

	public long getFileId() {
		return fileid.get();
	}

	public long getOffset() {
		return offset.get();
	}

	public boolean containsAtomId(byte needle) {

		byte[] atoms = data.getBytes();
		for (int i = 0; i < data.getLength(); i++) {
			if (atoms[i] == needle)
				return true;
		}
		return false;
	}

	public boolean isAssert() {
		return type.get() == GumboMessageType.ASSERT;
	}
	
	public boolean isRequest() {
		return type.get() == GumboMessageType.REQUEST;
	}
	
	public boolean isConfirm() {
		return type.get() == GumboMessageType.CONFIRM;
	}
	
	public boolean isData() {
		return type.get() == GumboMessageType.DATA;
	}
	
	
	public void setAssert(byte[] atomids, int length) {
		this.type.set(GumboMessageType.ASSERT);
		this.data.set(atomids, 0, length);

	}
	
	public void setRequest(long fileid, long offset, byte[] atomids, int length) {
		this.type.set(GumboMessageType.REQUEST);
		this.fileid.set(fileid);
		this.offset.set(offset);
		this.data.set(atomids, 0, length);
	}
	
	
	public void setConfirm(byte[] atomids, int length) {
		this.type.set(GumboMessageType.CONFIRM);
		this.data.set(atomids, 0, length);

	}
	
	public void setData(byte[] data, int length) {
		this.type.set(GumboMessageType.DATA);
		this.data.set(data, 0, length);
	}
	
	
	public void setType(byte type) {
		this.type.set(type);
		
	}
	
	public void setDataBytes(byte[] data, int length) {
		this.data.set(data, 0, length);
	}
	


	public BytesWritable getData() {
		return data;
	}
	

	public GumboMessageWritable duplicate() {
		return new GumboMessageWritable(type.get(), fileid.get(), offset.get(), data.getBytes(), data.getLength());
	}

	public void getAddressBytes(BytesWritable bw) {
		
		if (buffer == null)
			buffer = new DataOutputBuffer(32);
		else
			buffer.reset();
		
		try {
			fileid.write(buffer);
			offset.write(buffer);
		} catch (IOException e) {
			// should not fail
			LOG.error("IO error!" + System.lineSeparator() + e.getMessage() );
			e.printStackTrace();
		}
		byte[] data = buffer.getData();
		int dataLength = buffer.getLength();
		bw.set(data, 0, dataLength);
		
	}

	public boolean containsAtomId(int i) {
		return containsAtomId((byte) i);
	}

	




}
