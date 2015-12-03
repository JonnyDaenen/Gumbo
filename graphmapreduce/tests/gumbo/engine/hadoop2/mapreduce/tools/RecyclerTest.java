package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.*;

import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.WritableBuffer;

public class RecyclerTest {
	
	@Test
	public void testReycle() {
		WritableBuffer<GumboMessageWritable> buf = new WritableBuffer<>();

		GumboMessageWritable gw = new GumboMessageWritable();
		byte [] atomids = {1,2,3};
		gw.setRequest(0, 1, atomids, 3);
		GumboMessageWritable gw2 = buf.addWritable(gw);
		
		assertFalse(gw == gw2);

		// clear the buffer, internal objects are kept
		buf.clear();
		
		GumboMessageWritable gw3 = new GumboMessageWritable();
		byte [] atomids3 = {4,5,6};
		gw3.setRequest(100, 200, atomids3, 3);
		GumboMessageWritable gw4 = buf.addWritable(gw3);
		GumboMessageWritable gw5 = buf.addWritable(gw3);
		
		// the same object is re-used, so gw2 is adjusted
		assertTrue(gw2 == gw4);
		assertFalse(gw3 == gw4);
		assertFalse(gw4 == gw5);
	}

}
