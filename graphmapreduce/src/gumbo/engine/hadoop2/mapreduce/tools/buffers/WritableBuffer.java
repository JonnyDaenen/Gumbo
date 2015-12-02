package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Keeps a list of writables for later use.
 * The internal writables are kept to avoid excess memory allocation
 * 
 * @author Jonny Daenen
 *
 */
public class WritableBuffer {
	
	List<Writable> buffer;
	int used;
	
	public WritableBuffer() {
		used = 0;
		buffer = new ArrayList<>(10);
		setCapacity(10);
	}
	
	private void setCapacity(int i) {
		
		if (buffer.size() < i) {
			addWritables(3 * i / 2 - buffer.size());
		}
		
	}

	private void addWritables(int i) {
		// TODO Auto-generated method stub
		
	}

	public int size() {
		return used;
	}

}
