package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;

public class ConfirmBuffer {

	boolean [] atomids;
	GumboMessageWritable data;

	QuickWrappedTuple qt;


	public ConfirmBuffer(int maxAtomID) {
		atomids = new boolean[maxAtomID+1];
		qt = new QuickWrappedTuple();
		data = new GumboMessageWritable();
	}


	/**
	 * Sets the guard tuple data. A copy is made.
	 * @param value a message containing the guard tuple data
	 */
	public void setMessage(GumboMessageWritable value) {
		// copy content data to local buffer object
		data.set(value);
	}

	/**
	 * Activates atom ids in the message.
	 * @param value a message containing atom ids.
	 */
	public void addAtomIDs(GumboMessageWritable value) {

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
	 * Deactivates all atom ids and truncates the data buffer.
	 */
	public void reset() {
		clearAtomIds();
		data.setGarbage();
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
	public boolean load(TupleEvaluator pi, Text output) {

		if (data.isGarbage())
			return false;
		
		byte [] rawbytes = data.getData().getBytes();
		qt.initialize(rawbytes, data.getData().getLength());
		return pi.project(data.getQueryIds(), qt, output, atomids);

	}

	public boolean containsAtomID(int id) {
		if (atomids.length <= id)
			return false;
		return atomids[id];
	}

}
