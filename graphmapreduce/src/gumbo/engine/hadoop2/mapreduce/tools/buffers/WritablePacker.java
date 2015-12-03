package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable.MessageMergeException;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * 
 * @author Jonny Daenen
 *
 */
public class WritablePacker {



	HashMap<VBytesWritable, Set<GumboMessageWritable>> buffer;
	WritableBuffer<GumboMessageWritable> messageFactory;
	WritableBuffer<VBytesWritable> bytesFactory;

	public WritablePacker(int initsize) {
		buffer = new HashMap<>(initsize);
		messageFactory = new WritableBuffer<>();
		bytesFactory = new WritableBuffer<>();
	}


	/**
	 * Adds a KV pair to the buffer.
	 * These are stored in recycled objects.
	 * 
	 * @param key key bytes
	 * @param value value message
	 */
	public void add(VBytesWritable key, GumboMessageWritable value) {


		Set<GumboMessageWritable> targetSet = buffer.get(key);
		if (targetSet == null) {
			targetSet = new HashSet<>();
			VBytesWritable bytesW = bytesFactory.addWritable(key);
			buffer.put(bytesW, targetSet);
		}
		GumboMessageWritable gumboW = messageFactory.addWritable(value);
		targetSet.add(gumboW);	
	}

	/**
	 * Merges the messages per key.
	 * @return a set of KV pairs with merged values
	 */
	public Set<Pair<VBytesWritable, GumboMessageWritable>> pack() {
		Set<Pair<VBytesWritable, GumboMessageWritable>> resultset = new HashSet<>();
		HashSet<GumboMessageWritable> remainder = new HashSet<>();

		// for each key
		for (VBytesWritable key : buffer.keySet()) {

			// merge all messages into one
			Set<GumboMessageWritable> valueSet = buffer.get(key); // OPTIMIZE use list instead of set
			while (!valueSet.isEmpty()) {
				Iterator<GumboMessageWritable> it = valueSet.iterator();
				remainder = new HashSet<>();

				// try to merge all with the first message.
				GumboMessageWritable goal = it.next();
				while (it.hasNext()){
					GumboMessageWritable current = it.next();
					try {
						goal.merge(current);
					} catch (MessageMergeException e) {
						// merging failed, so keep message for later
						remainder.add(current);
					}
				}

				resultset.add(new Pair<>(key, goal));

				// continue with the remaining messages
				valueSet = remainder;
			}
		}

		return resultset;

	}

	/**
	 * Clears all KV pairs.
	 * Internal recyclable objects may be kept.
	 */
	public void clear() {
		buffer.clear();
		messageFactory.clear();
		bytesFactory.clear();
	}


}
