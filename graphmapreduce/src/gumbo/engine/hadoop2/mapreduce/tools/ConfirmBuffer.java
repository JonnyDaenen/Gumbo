package gumbo.engine.hadoop2.mapreduce.tools;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;

public class ConfirmBuffer {
	
	boolean [] atomids;
	byte [] data;
	int length;

	QuickWrappedTuple qt;
	
	
	public ConfirmBuffer(int numAtoms) {
		atomids = new boolean[numAtoms];
		qt = new QuickWrappedTuple();
		setCapacity(64);
	}

	/**
	 * Sets the capacity of the internal data buffer.
	 * @param i
	 */
	private void setCapacity(int i) {
		if (data == null || data.length < i) {
			data = new byte[i];
		}
		length = i;
	}

	/**
	 * Sets the guard tuple data.
	 * @param value a message containing the guard tuple data
	 */
	public void setMessage(GumboMessageWritable value) {
		// copy content data to local buffer
		BytesWritable bw = value.getContent();
		setCapacity(bw.getLength());
		System.arraycopy(bw.getBytes(), 0, data, 0, length);
	}

	/**
	 * Activates atom ids in the message.
	 * @param value a message containing atom ids.
	 */
	public void addAtomIDs(GumboMessageWritable value) {
		
		BytesWritable bw = value.getAtomIDBytes();
		byte [] ids = bw.getBytes();
		int size = bw.getLength();
		
		// activate atom ids that are provided
		for (int i = 0; i < size; i++) {
			this.atomids[ids[i]] = true;
		}
	}

	/**
	 * Deactivates all atom ids and truncates the data buffer.
	 */
	public void reset() {
		clearAtomIds();
		setCapacity(0);
	}

	
	/**
	 * Clears all atom ids.
	 */
	private void clearAtomIds() {
		for (int i = 0; i < atomids.length; i++) { 
			this.atomids[i] = false;
		}
	}

	/**
	 * If the query evaluates to true, the projection of the guard data is loaded in the output writable.
	 * @param pi
	 * @param output
	 * @return
	 */
	public boolean load(GuardEvaluator pi, Text output) {
		boolean eval = pi.eval(atomids);
		
		if (eval) {
			qt.initialize(data);
			pi.project(data, output);
		}
		
		// TODO implement
		return eval;
	}

}
