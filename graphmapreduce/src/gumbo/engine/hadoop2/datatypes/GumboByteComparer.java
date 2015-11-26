package gumbo.engine.hadoop2.datatypes;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable.Comparator;
import sun.misc.Unsafe;

@SuppressWarnings("unused") // used via reflection
public class GumboByteComparer extends WritableComparator {

	static final Unsafe theUnsafe;

	/** The offset to the first element in a byte array. */
	static final int BYTE_ARRAY_BASE_OFFSET;

	static {
		theUnsafe = (Unsafe) AccessController.doPrivileged(
				new PrivilegedAction<Object>() {
					@Override
					public Object run() {
						try {
							Field f = Unsafe.class.getDeclaredField("theUnsafe");
							f.setAccessible(true);
							return f.get(null);
						} catch (NoSuchFieldException e) {
							// It doesn't matter what we throw;
							// it's swallowed in getBestComparer().
							throw new Error();
						} catch (IllegalAccessException e) {
							throw new Error();
						}
					}
				});

		BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

		// sanity check - this should never fail
		if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
			throw new AssertionError();
		}
		

		WritableComparator.define(BytesWritable.class, new GumboByteComparer());
	}
	static final boolean littleEndian =
			ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);


	/**
	 * Returns true if x1 is less than x2, when both values are treated as
	 * unsigned.
	 */
	static boolean lessThanUnsigned(long x1, long x2) {
		return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
	}


	public int compare(byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		
		// Short circuit equal case
		if (buffer1 == buffer2 &&
				offset1 == offset2 &&
				length1 == length2) {
			return 0;
		}
		int minLength = Math.min(length1, length2);
		int minWords = minLength / Longs.BYTES;
		int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
		int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

		/*
		 * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
		 * time is no slower than comparing 4 bytes at a time even on 32-bit.
		 * On the other hand, it is substantially faster on 64-bit.
		 */
		for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
			long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
			long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
			long diff = lw ^ rw;

			if (diff != 0) {
				if (!littleEndian) {
					return lessThanUnsigned(lw, rw) ? -1 : 1;
				}

				// Use binary search
				int n = 0;
				int y;
				int x = (int) diff;
				if (x == 0) {
					x = (int) (diff >>> 32);
					n = 32;
				}

				y = x << 16;
				if (y == 0) {
					n += 16;
				} else {
					x = y;
				}

				y = x << 8;
				if (y == 0) {
					n += 8;
				}
				return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
			}
		}

		// The epilogue to cover the last (minLength % 8) elements.
		for (int i = minWords * Longs.BYTES; i < minLength; i++) {
			int a = (buffer1[offset1+i] & 0xff);
			int b = (buffer2[offset2+i] & 0xff);
			if (a != b) {
				return a - b;
			}
		}



		return length1 - length2;
	}
}
