package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps a list of objects that can be recycled.
 * The internal writables are kept to avoid excess memory allocation
 * 
 * @author Jonny Daenen
 *
 * @param <A> recyclable object
 */
public class WritableBuffer<A extends Recyclable<A>> {
	
	int MAX_REMAINING = 0;

	List<A> buffer;
	int used;

	public WritableBuffer() {
		used = 0;
		buffer = new ArrayList<>(10);
	}
	
	public WritableBuffer(int expectedSize) {
		used = 0;
		buffer = new ArrayList<>(expectedSize);
		MAX_REMAINING = expectedSize;
	}



	/**
	 * Copies an object to an internal writable.
	 * 
	 * @param writable the writable to copy
	 * @return the internal writable
	 */
	public A addWritable(A writable) {
		// update count
		used++;
		A cached;

		// adjust capacity
		if (buffer.size() >= used) {
			// request internal object
			cached = buffer.get(used - 1);

			// copy object
			cached.set(writable);

			// return internal representation
			return cached;
		} else {
			cached = writable.duplicate();
			buffer.add(cached);
		}

		return cached;

	}

	/**
	 * Returns the number of buffered objects.
	 * @return number of objects in the buffer
	 */
	public int size() {
		return used;
	}

	/**
	 * Empties the buffer, internal object remain.
	 * The number of remaining objects can be bounded using
	 * {@link #setMaxRemaining(int)}.
	 */
	public void clear() {
		trimBuffer();
		used = 0;
	}
	
	/**
	 * Trims the buffer to maximal remaining elements, or to the number of 
	 * used elements if this has a higher value. 
	 * Has no effect when max remaining elements equals 0.
	 */
	private void trimBuffer() {
		// buffer remains intact
		if (MAX_REMAINING == 0 || buffer.size() <= MAX_REMAINING ) {
			return;
		}
		

		// copy enough elements into new buffer.
		List<A> newBuffer = new ArrayList<>(MAX_REMAINING);
		
		for (int i = 0; i < Math.max(used, MAX_REMAINING); i++) {
			newBuffer.add(buffer.get(i));
		}
		
		
		buffer = newBuffer;
	}



	/**
	 * Sets the maximal objects that may remain after a clear operation.
	 * When the number of objects is larger, the excess ones will be removed.
	 * When this is set to 0, nothing will be removed.
	 */
	public void setMaxRemaining(int maxRemaining) {
		MAX_REMAINING = maxRemaining;
	}
}
