package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.RequestBuffer;

public class RequestBufferTest {
	
	@Test
	public void requestTest() {
		
		// prepare some requests
		GumboMessageWritable gm1 = new GumboMessageWritable();
		byte [] atoms1 = {1,2,3};
		gm1.setRequest(10, 100, atoms1, atoms1.length );
		
		GumboMessageWritable gm2 = new GumboMessageWritable();
		byte [] atoms2 = {3,4,5};
		gm2.setRequest(10, 100, atoms2, atoms2.length);
		
		GumboMessageWritable gm3 = new GumboMessageWritable();
		byte [] atoms3 = {1,3,5};
		gm3.setAssert(atoms3, atoms3.length);
		
		RequestBuffer rb = new RequestBuffer(5);
		rb.addMessage(gm1);
		rb.addMessage(gm2);
		rb.addAtomIds(gm3);

		assertFalse("atom presence 0", rb.containsAtomID(0));
		assertTrue("atom presence 1", rb.containsAtomID(1));
		assertFalse("atom presence 2", rb.containsAtomID(2));
		assertTrue("atom presence 3", rb.containsAtomID(3));
		assertFalse("atom presence 4", rb.containsAtomID(4));
		assertTrue("atom presence 5", rb.containsAtomID(5));
		assertFalse("OOB atom presence 6", rb.containsAtomID(6));
		
		
		VBytesWritable bw = new VBytesWritable();
		GumboMessageWritable gw = new GumboMessageWritable();
		rb.load(1, bw, gw);
		
		ByteArrayInputStream bs = new ByteArrayInputStream(bw.getBytes(), 0, bw.getLength());
		DataInputStream ds = new DataInputStream(bs);
		VLongWritable vl = new VLongWritable();
		try {
			vl.readFields(ds);
			long fileid = vl.get();
			
			vl.readFields(ds);
			long offset = vl.get();
			
			assertEquals("fileid correct mapping", 10, fileid);
			assertEquals("fileid correct mapping", 100, offset);
			
			
		} catch (IOException e) {
			fail();
		}
		
		assertTrue("Confirm type", gw.isConfirm());
		assertFalse("atom id output message", gw.containsAtomId(0));
		assertFalse("atom id output message", gw.containsAtomId(1));
		assertFalse("atom id output message", gw.containsAtomId(2));
		assertTrue("atom id output message", gw.containsAtomId(3));
		assertFalse("atom id output message", gw.containsAtomId(4));
		assertTrue("atom id output message", gw.containsAtomId(5));
		assertFalse("OOB atom id output message", gw.containsAtomId(6));
		
		
		
	}

}
