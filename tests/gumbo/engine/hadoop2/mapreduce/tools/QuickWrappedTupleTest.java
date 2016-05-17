package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class QuickWrappedTupleTest {
	
	
	@Test
	public void testParser() {
		Text t1 = new Text("1233445,123123,14121241,124");
		Text t2 = new Text("555,55555");
		
		String[] t1parts = t1.toString().split(",");
		String[] t2parts = t2.toString().split(",");
		
		// test basic operations
		QuickWrappedTuple qt = new QuickWrappedTuple();
		qt.initialize(t1);

		assertEquals("correct start", 0, qt.getStart(0));
		assertEquals("correct length", 7, qt.getLength(0));
		assertEquals("correct start", 24, qt.getStart(3));
		assertEquals("correct length", 3, qt.getLength(3));
		assertEquals("correct number of fields", 4, qt.size());
		
		assertEquals("byte string length", t1.getLength(), qt.getLength());
		assertEquals("length equality", t1parts[1].length(), qt.getLength(1));
		assertEquals("number of parts", t1parts.length, qt.size());
		

		assertEquals("number of parts", t1parts[1], new String(qt.getData(), qt.getStart(1), qt.getLength(1)));
		
		// test re-use
		qt.initialize(t2);
		
		assertEquals("correct start", 0, qt.getStart(0));
		assertEquals("correct length", 3, qt.getLength(0));
		assertEquals("correct start", 4, qt.getStart(1));
		assertEquals("correct length", 5, qt.getLength(1));
		assertEquals("correct number of fields", 2, qt.size());
		
		assertEquals("byte string length", t2.getLength(), qt.getLength());
		assertEquals("length equality", t2parts[1].length(), qt.getLength(1));
		assertEquals("number of parts", t2parts.length, qt.size());
		
		
		
		
		
	}

}
