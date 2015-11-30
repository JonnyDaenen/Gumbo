package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;

public class GumboMessageTest {


	@Test
	public void assertTest(){

		byte [] atomids = {1,2,3,4};

		// create
		GumboMessageWritable gm = new GumboMessageWritable();
		gm.setAssert(atomids, atomids.length);

		// serialize
		byte[] serBytes = serialize(gm);

		// deserialize
		GumboMessageWritable gm2 = new GumboMessageWritable();
		deserialize(gm2, serBytes);

		// inspect content
		byte [] newatomids = gm2.getData().getBytes();
		assertTrue("atom id length is " + newatomids.length , atomids.length < newatomids.length);
		for (int i = 0; i < atomids.length; i++) {
			assertEquals("atom id equals", atomids[i], newatomids[i]);
		}
		
		assertTrue("correct Type", gm2.isAssert());
		assertFalse("correct Type", gm2.isRequest());
		assertFalse("correct Type", gm2.isConfirm());
		assertFalse("correct Type", gm2.isData());


		// check equality
		assertEquals("writable equals", gm, gm2);
		assertEquals("writable compare", 0, gm.compareTo(gm2));

	}


	@Test
	public void requestTest(){

		byte [] atomids = {1,2,3,4};
		long offset = 40000000;
		long fileid = 12121212;

		// create
		GumboMessageWritable gm = new GumboMessageWritable();
		gm.setRequest(fileid, offset, atomids, atomids.length);

		// serialize
		byte[] serBytes = serialize(gm);

		// deserialize
		GumboMessageWritable gm2 = new GumboMessageWritable();
		deserialize(gm2, serBytes);

		// inspect content
		byte [] newatomids = gm2.getData().getBytes();
		assertTrue("atom id length is " + newatomids.length , atomids.length < newatomids.length);
		for (int i = 0; i < atomids.length; i++) {
			assertEquals("atom id equals", atomids[i], newatomids[i]);
		}

		assertEquals("correct file id", gm2.getFileId(), fileid);
		assertEquals("correct offset", gm2.getOffset(), offset);
		
		assertFalse("correct Type", gm2.isAssert());
		assertTrue("correct Type", gm2.isRequest());
		assertFalse("correct Type", gm2.isConfirm());
		assertFalse("correct Type", gm2.isData());


		// check equality
		assertEquals("writable equals", gm, gm2);
		assertEquals("writable compare", 0, gm.compareTo(gm2));

	}


	@Test
	public void confirmTest(){

		byte [] atomids = {1,2,3,4};

		// create
		GumboMessageWritable gm = new GumboMessageWritable();
		gm.setConfirm(atomids, atomids.length);

		// serialize
		byte[] serBytes = serialize(gm);

		// deserialize
		GumboMessageWritable gm2 = new GumboMessageWritable();
		deserialize(gm2, serBytes);

		// inspect content
		byte [] newatomids = gm2.getData().getBytes();
		assertTrue("atom id length is " + newatomids.length , atomids.length < newatomids.length);
		for (int i = 0; i < atomids.length; i++) {
			assertEquals("atom id equals", atomids[i], newatomids[i]);
		}
		
		assertFalse("correct Type", gm2.isAssert());
		assertFalse("correct Type", gm2.isRequest());
		assertTrue("correct Type", gm2.isConfirm());
		assertFalse("correct Type", gm2.isData());


		// check equality
		assertEquals("writable equals", gm, gm2);
		assertEquals("writable compare", 0, gm.compareTo(gm2));

	}


	@Test
	public void dataTest(){


		byte [] atomids = {1,2,3,4};

		// create
		GumboMessageWritable gm = new GumboMessageWritable();
		gm.setData(atomids, atomids.length);

		// serialize
		byte[] serBytes = serialize(gm);

		// deserialize
		GumboMessageWritable gm2 = new GumboMessageWritable();
		deserialize(gm2, serBytes);

		// inspect content
		byte [] newatomids = gm2.getData().getBytes();
		assertTrue("atom id length is " + newatomids.length , atomids.length < newatomids.length);
		for (int i = 0; i < atomids.length; i++) {
			assertEquals("atom id equals", atomids[i], newatomids[i]);
		}
		
		assertFalse("correct Type", gm2.isAssert());
		assertFalse("correct Type", gm2.isRequest());
		assertFalse("correct Type", gm2.isConfirm());
		assertTrue("correct Type", gm2.isData());


		// check equality
		assertEquals("writable equals", gm, gm2);
		assertEquals("writable compare", 0, gm.compareTo(gm2));

	}


	public static byte[] serialize(Writable writable) { 
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream(); 
			DataOutputStream dataOut = new DataOutputStream(out); 
			writable.write(dataOut);
			dataOut.close();
			return out.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static byte[] deserialize(Writable writable, byte[] bytes)  {
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(bytes); 
			DataInputStream dataIn = new DataInputStream(in); 
			writable.readFields(dataIn);
			dataIn.close();
			return bytes;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

}
