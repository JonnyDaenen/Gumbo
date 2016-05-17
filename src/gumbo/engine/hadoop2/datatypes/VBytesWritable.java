package gumbo.engine.hadoop2.datatypes;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import gumbo.engine.hadoop2.mapreduce.tools.buffers.Recyclable;

/** 
 * A byte sequence that is usable as a key or value.
 * It is resizable and distinguishes between the size of the sequence and
 * the current capacity. The hash function is the front of the md5 of the 
 * buffer. The sort order is the same as memcmp.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VBytesWritable extends BinaryComparable
implements WritableComparable<BinaryComparable>, Recyclable<VBytesWritable>{
	private static final byte[] EMPTY_BYTES = {};

	private int size;
	private byte[] bytes;

	/**
	 * Create a zero-size sequence.
	 */
	public VBytesWritable() {this(EMPTY_BYTES);}

	/**
	 * Create a BytesWritable using the byte array as the initial value.
	 * @param bytes This array becomes the backing storage for the object.
	 */
	public VBytesWritable(byte[] bytes) {
		this(bytes, bytes.length);
	}

	/**
	 * Create a BytesWritable using the byte array as the initial value
	 * and length as the length. Use this constructor if the array is larger
	 * than the value it represents.
	 * @param bytes This array becomes the backing storage for the object.
	 * @param length The number of bytes to use from array.
	 */
	public VBytesWritable(byte[] bytes, int length) {
		this.bytes = bytes;
		this.size = length;
	}

	/**
	 * Get a copy of the bytes that is exactly the length of the data.
	 * See {@link #getBytes()} for faster access to the underlying array.
	 */
	public byte[] copyBytes() {
		byte[] result = new byte[size];
		System.arraycopy(bytes, 0, result, 0, size);
		return result;
	}

	/**
	 * Get the data backing the BytesWritable. Please use {@link #copyBytes()}
	 * if you need the returned array to be precisely the length of the data.
	 * @return The data is only valid between 0 and getLength() - 1.
	 */
	@Override
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Get the data from the BytesWritable.
	 * @deprecated Use {@link #getBytes()} instead.
	 */
	@Deprecated
	public byte[] get() {
		return getBytes();
	}

	/**
	 * Get the current size of the buffer.
	 */
	@Override
	public int getLength() {
		return size;
	}

	/**
	 * Get the current size of the buffer.
	 * @deprecated Use {@link #getLength()} instead.
	 */
	@Deprecated
	public int getSize() {
		return getLength();
	}

	/**
	 * Change the size of the buffer. The values in the old range are preserved
	 * and any new values are undefined. The capacity is changed if it is 
	 * necessary.
	 * @param size The new number of bytes
	 */
	public void setSize(int size) {
		if (size > getCapacity()) {
			setCapacity(size * 3 / 2);
		}
		this.size = size;
	}

	/**
	 * Get the capacity, which is the maximum size that could handled without
	 * resizing the backing storage.
	 * @return The number of bytes
	 */
	public int getCapacity() {
		return bytes.length;
	}

	/**
	 * Change the capacity of the backing storage.
	 * The data is preserved.
	 * @param new_cap The new capacity in bytes.
	 */
	public void setCapacity(int new_cap) {
		if (new_cap != getCapacity()) {
			byte[] new_data = new byte[new_cap];
			if (new_cap < size) {
				size = new_cap;
			}
			if (size != 0) {
				System.arraycopy(bytes, 0, new_data, 0, size);
			}
			bytes = new_data;
		}
	}

	/**
	 * Set the BytesWritable to the contents of the given newData.
	 * @param newData the value to set this BytesWritable to.
	 */
	public void set(VBytesWritable newData) {
		set(newData.bytes, 0, newData.size);
	}

	/**
	 * Set the value to a copy of the given byte range
	 * @param newData the new values to copy in
	 * @param offset the offset in newData to start at
	 * @param length the number of bytes to copy
	 */
	public void set(byte[] newData, int offset, int length) {
		setSize(0);
		setSize(length);
		System.arraycopy(newData, offset, bytes, 0, size);
	}

	// inherit javadoc
	@Override
	public void readFields(DataInput in) throws IOException {
		//    setSize(0); // clear the old data
		//    setSize(in.readInt());
		setSize(WritableUtils.readVInt(in));
		in.readFully(bytes, 0, size);
	}

	// inherit javadoc
	@Override
	public void write(DataOutput out) throws IOException {
		//    out.writeInt(size);
		WritableUtils.writeVInt(out, size);
		out.write(bytes, 0, size);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	/**
	 * Are the two byte sequences equal?
	 */
	@Override
	public boolean equals(Object right_obj) {
		if (right_obj instanceof VBytesWritable)
			return super.equals(right_obj);
		return false;
	}

	/**
	 * Generate the stream of bytes as hex pairs separated by ' '.
	 */
	@Override
	public String toString() { 
		StringBuilder sb = new StringBuilder(3*size);

		//    sb.append(size);
		//    sb.append(' ');
		for (int idx = 0; idx < size; idx++) {
			// if not the first, put a blank separator in
			if (idx != 0) {
				sb.append(' ');
			}
			String num = Integer.toHexString(0xff & bytes[idx]);
			// if it is only one digit, add a leading 0.
			if (num.length() < 2) {
				sb.append('0');
			}
			sb.append(num);
		}
		return sb.toString();
	}

	/** A Comparator optimized for BytesWritable. */ 
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(VBytesWritable.class);
		}

		/**
		 * Compare the buffers in serialized form.
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1,
				byte[] b2, int s2, int l2) {

				
			
			
			
			// compare byte length
			if (l1 != l2)
				return l1 - l2;

			int i1 = 0; 
			int i2 = 0;

			// compare until 8-multiple remains
			// so that underlying overflow is not called

			int extra = l1 % 8;
			for (int i = 0; i < extra; i++){

				// ff conversion is necessary!
				i1 = (i1 << 8) | (b1[s1] & 0xFF);
				i2 = (i2 << 8) | (b2[s2] & 0xFF);
				
				// when int is full, we compare and reset
				if ( (i+1) % 3 == 0) {
					if (i1 != i2)
						return i1 - i2;
					i1 = 0;
					i2 = 0;
				}

				s1++;
				s2++;
			}

			// compare leftovers
			if (i1 != i2)
				return i1 - i2;

			if (l1 == extra)
				return 0;

			// adjust buffer sizes	
			l1 -= extra;
			l2 = l1;

			// full compare the rest
			return compareBytes(b1, s1, l1, 
					b2, s2, l2);
		}



	}

	static {                                        // register this comparator
		WritableComparator.define(VBytesWritable.class, new Comparator());
	}

	@Override
	public VBytesWritable duplicate() {
		VBytesWritable b = new VBytesWritable();
		b.set(this);
		return b;
	}

	@Override
	public int compareTo(BinaryComparable other) {
		if (this == other)
			return 0;
		return VBytesWritable.Comparator.compareBytes(getBytes(), 0, getLength(),
				other.getBytes(), 0, other.getLength());
	}

}
