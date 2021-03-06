package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import java.nio.ByteBuffer;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;

public class RequestBuffer {

	private boolean [] atomids;
	private RecyclableBuffer<GumboMessageWritable> buffer;
	private ByteBuffer atombytes;

	public RequestBuffer(int maxAtomID) {
		clearAtomsIds(maxAtomID + 1);
	}

	public void clearAtomsIds(int maxAtomID) {
		atomids = new boolean[maxAtomID];
		buffer = new RecyclableBuffer<>(32);

		atombytes = ByteBuffer.wrap(new byte[maxAtomID]);
	}

	/**
	 * Tracks the atom ids present in the message.
	 * 
	 * @param value a message
	 */
	public void addAtomIds(GumboMessageWritable value) {

		// get Atom id bytes
		VBytesWritable bw = value.getData();
		byte [] ids = bw.getBytes();
		int size = bw.getLength();

		// activate atom ids that are provided
		for (int i = 0; i < size; i++) {

			if (ids[i] >= atomids.length)
				continue;

			this.atomids[ids[i]] = true;
		}
	}

	/**
	 * Loads buffer item i into two writables.
	 * 
	 * @param i buffer item position
	 * @param bw key bytes to be filled
	 * @param gw value message to be filled
	 * @return true iff the writables were set, false if the writables should not be written to output
	 */
	public boolean load(int i, VBytesWritable bw, GumboMessageWritable gw) {

		GumboMessageWritable current = buffer.get(i);

		// create tuple id
		// OPTIMIZE use internal bytewritable
		current.getAddressBytes(bw);

		// filter atom ids and put result in atombytes
		if (filter(current)) {
			// create confirm message
			gw.setConfirm(atombytes.array(), atombytes.position());
			return true;
		}

		return false; 
	}

	/**
	 * Updates internal byte buffer with matching atom ids.
	 * Atom ids are taken from the given matches and 
	 * intersected with the internal atom id buffer.
	 * 
	 * @param current the message containing the atom ids
	 * @return true iff the intersection is non-empty
	 */
	private boolean filter(GumboMessageWritable current) {

		VBytesWritable bw = current.getData();
		byte [] ids = bw.getBytes();
		int size = bw.getLength();

		atombytes.clear();

		boolean remaining = false;

		// activate atom ids that are provided
		for (int i = 0; i < size; i++) {
			if (this.atomids[ids[i]]) {
				atombytes.put(ids[i]);
				remaining = true;
			}
		}

		return remaining;

	}

	/**
	 * Buffers a copy of the message.
	 * 
	 * @param value the message
	 */
	public void addMessage(GumboMessageWritable value) {
		// OPTIMIZE re-use older buffer elements

//		GumboMessageWritable val = value.duplicate();
		buffer.addWritable(value);
	}


	/**
	 * Calculates the number of buffered messages.
	 * @return size of the message buffer
	 */
	public int size() {
		return buffer.size();
	}

	/**
	 * Deactivates the internal atom ids and clears the message buffer.
	 */
	public void reset() {
		clearAtomIds();	
		buffer.clear();
	}


	/**
	 * Clears the internal atom id buffer.
	 */
	private void clearAtomIds() {
		for (int i = 0; i < atomids.length; i++) { 
			this.atomids[i] = false;
		}
	}

	public boolean containsAtomID(int id) {
		if (atomids.length <= id)
			return false;
		return atomids[id];
	}

}
