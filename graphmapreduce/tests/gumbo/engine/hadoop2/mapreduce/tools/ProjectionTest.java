package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.GuardProjection;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.GuardedProjection;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class ProjectionTest {


	@Test
	public void testGuard() {

		GFAtomicExpression guarded = new GFAtomicExpression("S", "y", "x");
		GFAtomicExpression guard = new GFAtomicExpression("R", "x", "y", "x");
		GuardProjection gp = new GuardProjection("R", 100, guard , guarded, (byte)1);

		QuickWrappedTuple qt = new QuickWrappedTuple();
		qt.initialize(new Text("1,2,1"));

		BytesWritable bw = new BytesWritable();
		GumboMessageWritable gw = new GumboMessageWritable();

		boolean result = gp.load(qt, 200, bw, gw);

		assertTrue("guard matches", result);

		assertEquals("correct file id", 100, gw.getFileId());
		assertEquals("correct offset", 200, gw.getOffset());
		assertTrue("correct type", gw.isRequest());
		assertEquals("correct atom id", 1, gw.getData().getBytes()[0]);
		assertEquals("correct length", 1, gw.getData().getLength());


		String key = new String(bw.getBytes(), 0, bw.getLength());
		assertEquals("correct key", "2,1", key);

		qt.initialize(new Text("1,1,1"));
		result = gp.load(qt, 200, bw, gw);
		assertTrue("guard matches", result);
		
		qt.initialize(new Text("1,2,3"));
		result = gp.load(qt, 200, bw, gw);
		assertFalse("guard does not match", result);

	}
	
	@Test
	public void testGuarded() {

		GFAtomicExpression guarded = new GFAtomicExpression("S", "x", "y", "x");
		GuardedProjection gp = new GuardedProjection("S", guarded, (byte)2);

		QuickWrappedTuple qt = new QuickWrappedTuple();
		qt.initialize(new Text("1,2,1"));

		BytesWritable bw = new BytesWritable();
		GumboMessageWritable gw = new GumboMessageWritable();

		boolean result = gp.load(qt, 0, bw, gw);

		assertTrue("guarded matches", result);

		assertTrue("correct type", gw.isAssert());
		assertEquals("correct atom id", 2, gw.getData().getBytes()[0]);
		assertEquals("correct length", 1, gw.getData().getLength());


		String key = new String(bw.getBytes(), 0, bw.getLength());
		assertEquals("correct key", "1,2,1", key);

		qt.initialize(new Text("1,1,1"));
		result = gp.load(qt, 0, bw, gw);
		assertTrue("guard matches", result);
		
		qt.initialize(new Text("1,2,3"));
		result = gp.load(qt, 0, bw, gw);
		assertFalse("guard does not match", result);

	}
}
