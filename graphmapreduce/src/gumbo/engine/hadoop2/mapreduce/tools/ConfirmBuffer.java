package gumbo.engine.hadoop2.mapreduce.tools;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.google.common.primitives.Bytes;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;

public class ConfirmBuffer {
	
	boolean [] atomids;
	byte [] data;
	int length;
	
	
	public ConfirmBuffer(int numAtoms) {
		atomids = new boolean[numAtoms];
		setCapacity(64);
	}

	private void setCapacity(int i) {
		if (data == null || data.length < i) {
			data = new byte[i];
		}
		length = i;
	}

	public void setMessage(GumboMessageWritable value) {
		// copy content data to local buffer
		BytesWritable bw = value.getContent();
		setCapacity(bw.getLength());
		System.arraycopy(bw.getBytes(), 0, data, 0, length);
	}

	public void addAtomIDs(GumboMessageWritable value) {
		
		BytesWritable bw = value.getAtomIDBytes();
		byte [] ids = bw.getBytes();
		int size = bw.getLength();
		
		// activate atom ids that are provided
		for (int i = 0; i < size; i++) {
			this.atomids[ids[i]] = true;
		}
	}

	public void reset() {
		clearAtomIds();
	}

	private void clearAtomIds() {
		for (int i = 0; i < atomids.length; i++) { 
			this.atomids[i] = false;
		}
	}

	public boolean load(TupleProjection pi, Text output) {
		// TODO implement
		return false;
	}

}
