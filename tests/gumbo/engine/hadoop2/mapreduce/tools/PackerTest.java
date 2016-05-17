package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.AssertRequestPacker;
import gumbo.structures.gfexpressions.io.Pair;

public class PackerTest {

	@Test 
	public void testMerge() {
		AssertRequestPacker p = new AssertRequestPacker(3);

		byte [] atomidskey1 = {1,2,3};
		VBytesWritable key1 = new VBytesWritable(atomidskey1, 3);

		byte [] atomidskey2 = {1,2,3};
		VBytesWritable key2 = new VBytesWritable(atomidskey2, 3);

		byte [] atomidskey3 = {4,5,6};
		VBytesWritable key3 = new VBytesWritable(atomidskey3, 3);


		GumboMessageWritable gw1 = new GumboMessageWritable();
		byte [] atomids1 = {1,2,3};
		gw1.setRequest(0, 1, atomids1, 3);

		GumboMessageWritable gw2 = new GumboMessageWritable();
		byte [] atomids2 = {4,5,1};
		gw2.setRequest(0, 1, atomids2, 3);

		GumboMessageWritable gw3 = new GumboMessageWritable();
		byte [] atomids3 = {7,8,9};
		gw3.setRequest(1, 1, atomids3, 3);

		GumboMessageWritable gw4 = new GumboMessageWritable();
		byte [] atomids4 = {10,11,12};
		gw4.setAssert(atomids4, 3);

		GumboMessageWritable gw5 = new GumboMessageWritable();
		byte [] atomids5 = {10,11,13};
		gw5.setAssert(atomids5, 3);

		GumboMessageWritable gw6 = new GumboMessageWritable();
		byte [] atomids6 = {1,2,3};
		gw6.setRequest(0, 1, atomids6, 3);

		// mergable requests
		p.add(key1, gw1);
		p.add(key1, gw2);
		Set<Pair<VBytesWritable, GumboMessageWritable>> result = p.pack();

		assertEquals(1, result.size()); // 1

		Pair<VBytesWritable, GumboMessageWritable> obj = result.iterator().next();
		VBytesWritable data = obj.snd.getData();
		assertEquals(6, data.getLength());

		assertEquals(1, data.getBytes()[0]);
		assertEquals(2, data.getBytes()[1]);
		assertEquals(3, data.getBytes()[2]);
		assertEquals(4, data.getBytes()[3]);
		assertEquals(5, data.getBytes()[4]);
		assertEquals(1, data.getBytes()[5]);

		// it is a new object
		assertFalse(obj.snd == gw1);
		assertFalse(obj.snd == gw2);
		assertFalse(obj.fst == key1);

		// different filename
		p.clear();
		p.add(key1, gw1);
		p.add(key1, gw3);
		result = p.pack();
		assertEquals(2, result.size()); // 2

		Iterator<Pair<VBytesWritable, GumboMessageWritable>> it = result.iterator();
		Pair<VBytesWritable, GumboMessageWritable> obj1 = it.next();
		Pair<VBytesWritable, GumboMessageWritable> obj2 = it.next();

		//		assertFalse(obj1.fst == obj2.fst);
		assertFalse(obj1.snd == obj2.snd);

		// incompatible types
		p.clear();
		p.add(key1, gw1);
		p.add(key1, gw4);
		result = p.pack();
		assertEquals(2, result.size()); // 2

		
		// asserts
		p.clear();
		p.add(key1, gw4);
		p.add(key1, gw5);
		result = p.pack();
		assertEquals(1, result.size()); // 1


		// mergable requests, equal keys in different objects
		p.clear();
		p.add(key1, gw1);
		p.add(key2, gw2);
		result = p.pack();
		assertEquals(1, result.size()); // 1

		
		// mergable requests, different keys
		p.clear();
		p.add(key1, gw1);
		p.add(key3, gw2);
		result = p.pack();
		assertEquals(2, result.size()); // 2

		
		// merging equal objects
		p.clear();
		p.add(key1, gw1);
		p.add(key1, gw6);
		result = p.pack();
		assertEquals(1, result.size()); // 1

		
		// merging the same objects
		p.clear();
		p.add(key1, gw1);
		p.add(key1, gw1);
		result = p.pack();
		assertEquals(1, result.size()); // 1

		


	}
}
