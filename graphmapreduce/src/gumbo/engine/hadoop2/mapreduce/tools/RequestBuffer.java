package gumbo.engine.hadoop2.mapreduce.tools;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;

public class RequestBuffer {
	
	boolean [] atomids;
	
	List<GumboMessageWritable> buffer;

	private ByteBuffer atombytes;

	public void clearAtomsIds(int numAtoms) {
		atomids = new boolean[numAtoms];
		buffer = new ArrayList<>(32);
		
		atombytes = ByteBuffer.wrap(new byte[numAtoms]);
	}

	public void addAtomIds(GumboMessageWritable value) {
		
		BytesWritable bw = value.getAtomIDBytes();
		byte [] ids = bw.getBytes();
		int size = bw.getLength();
		
		// activate atom ids that are provided
		for (int i = 0; i < size; i++) {
			this.atomids[ids[i]] = true;
		}
	}

	/**
	 * Loads buffer item i in two writables.
	 * The bytes object will contain the key and the
	 * message will be a Confirm message containing the atom ids.
	 * 
	 * Warning: the message is backed by a local byte array to save
	 * space and should <b>not</b> be changed. It will remain valid until
	 * the internal object is recycled, which can only happen after a reset.
	 * 
	 * @param i buffer item position
	 * @param bw target key bytes
	 * @param gw target value message
	 * @return TODO
	 */
	public boolean load(int i, BytesWritable bw, GumboMessageWritable gw) {
		
		GumboMessageWritable current = buffer.get(i);
		
		// create tuple id
		// OPTIMIZE use internal bytewritable
		bw.set(current.getAddressBytes());
		
		// filter atom ids and put result in atombytes
		if (filter(current)) {
			// create confirm message
			gw.setConfirm(atombytes.array(), atombytes.position());
			return true;
		}
		
		return false; 
	}

	private boolean filter(GumboMessageWritable current) {
		
		BytesWritable bw = current.getAtomIDBytes();
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

	public void addMessage(GumboMessageWritable value) {
		// OPTIMIZE re-use older buffer elements
		
		GumboMessageWritable val = value.duplicate();
		buffer.add(val);
	}

	public int size() {
		return buffer.size();
	}

	public void reset() {
		clearAtomIds();	
		buffer.clear();
	}
	
	private void clearAtomIds() {
		for (int i = 0; i < atomids.length; i++) { 
			this.atomids[i] = false;
		}
	}

}
