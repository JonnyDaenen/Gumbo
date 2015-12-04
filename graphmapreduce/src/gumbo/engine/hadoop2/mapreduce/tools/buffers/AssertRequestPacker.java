package gumbo.engine.hadoop2.mapreduce.tools.buffers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable.MessageMergeException;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * 
 * @author Jonny Daenen
 *
 */
public class AssertRequestPacker {



	RecyclableBuffer<GumboMessageWritable> messageFactory;
	RecyclableBuffer<VBytesWritable> bytesFactory;

	public AssertRequestPacker(int initsize) {
		messageFactory = new RecyclableBuffer<>();
		bytesFactory = new RecyclableBuffer<>();
	}


	/**
	 * Adds a KV pair to the buffer.
	 * These are stored in recycled objects.
	 * 
	 * @param key key bytes
	 * @param value value message
	 */
	public void add(VBytesWritable key, GumboMessageWritable value) {
		bytesFactory.addWritable(key);
		messageFactory.addWritable(value);
		
		// OPTIMIZE do packing here?
	}

	/**
	 * Merges the messages per key. 
	 * The returned writables refer to internals and should not be changed.
	 * 
	 * @return a set of KV pairs with merged values
	 */
	public Set<Pair<VBytesWritable, GumboMessageWritable>> pack() {


		for (int i = 0; i < bytesFactory.size(); i++) {

			VBytesWritable k1 = bytesFactory.get(i);
			GumboMessageWritable v1 = messageFactory.get(i);

			// skip allready merges messages
			if (v1.isGarbage())
				continue;

			// try to merge with all next messages
			for (int j = i+1; j < bytesFactory.size(); j++) {

				VBytesWritable k2 = bytesFactory.get(j);
				GumboMessageWritable v2 = messageFactory.get(j);

				if (v2.isGarbage())
					continue;


				try {
					v1.merge(v2);
					// if it worked, we mark the second message as garbage
					v2.setGarbage();
				} catch (MessageMergeException e) {
					// if it didn't work, we leave the message separate
				}

			}
		}

		Set<Pair<VBytesWritable, GumboMessageWritable>> resultset = new HashSet<>();

		for (int i = 0; i < bytesFactory.size(); i++) {

			VBytesWritable k1 = bytesFactory.get(i);
			GumboMessageWritable v1 = messageFactory.get(i);

			// skip allready merges messages
			if (!v1.isGarbage()) {
				Pair<VBytesWritable, GumboMessageWritable> p;
				p = new Pair<>(k1, v1);
				resultset.add(p);
			}

		}

		return resultset;

	}

	/**
	 * Clears all KV pairs.
	 * Internal recyclable objects may be kept.
	 */
	public void clear() {
		messageFactory.clear();
		bytesFactory.clear();
	}


}
