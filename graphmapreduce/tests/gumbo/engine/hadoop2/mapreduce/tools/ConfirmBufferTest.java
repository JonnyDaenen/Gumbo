package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.buffers.ConfirmBuffer;
import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class ConfirmBufferTest {

	@Test
	public void atomTest() {

		// prepare some confirms
		GumboMessageWritable gm1 = new GumboMessageWritable();
		byte [] atoms1 = {1,2,3};
		gm1.setConfirm(atoms1, atoms1.length );

		GumboMessageWritable gm2 = new GumboMessageWritable();
		byte [] atoms2 = {3,4,5};
		gm2.setConfirm(atoms2, atoms2.length);

		GumboMessageWritable gm3 = new GumboMessageWritable();
		Text t = new Text("11,22,33");
		gm3.setData(t.getBytes(), t.getLength());


		ConfirmBuffer cb = new ConfirmBuffer(5);
		cb.addAtomIDs(gm1);
		cb.addAtomIDs(gm2);
		cb.setMessage(gm3);


		assertFalse("atom presence 0", cb.containsAtomID(0));
		assertTrue("atom presence 1", cb.containsAtomID(1));
		assertTrue("atom presence 2", cb.containsAtomID(2));
		assertTrue("atom presence 3", cb.containsAtomID(3));
		assertTrue("atom presence 4", cb.containsAtomID(4));
		assertTrue("atom presence 5", cb.containsAtomID(5));
		assertFalse("OOB atom presence 6", cb.containsAtomID(6));

	}

	@Test
	public void projectTest() {
		GFAtomicExpression guard = new GFAtomicExpression("R", "x", "y", "x", "z");
		GFAtomicExpression out = new GFAtomicExpression("O", "y", "z", "x", "z");

		GFAtomicExpression guarded1 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression guarded2 = new GFAtomicExpression("T", "x", "x");

		GFAndExpression child = new GFAndExpression(guarded1, guarded2);

		GFExistentialExpression e = new GFExistentialExpression(guard, child, out);

		Map<GFAtomicExpression, Integer> atomids;
		atomids = new HashMap<GFAtomicExpression, Integer>();
		atomids.put(guard, 1);
		atomids.put(guarded1, 4);
		atomids.put(guarded2, 6);
		atomids.put(out, 5);

		TupleEvaluator te = new TupleEvaluator(e, "outfile.txt", atomids);

		// prepare some confirms
		GumboMessageWritable gm1 = new GumboMessageWritable();
		byte [] atoms1 = {1,4,3};
		gm1.setConfirm(atoms1, atoms1.length );

		GumboMessageWritable gm2 = new GumboMessageWritable();
		byte [] atoms2 = {1,6,5};
		gm2.setConfirm(atoms2, atoms2.length);

		GumboMessageWritable gm3 = new GumboMessageWritable();
		Text t = new Text("11,22,33,44");
		gm3.setData(t.getBytes(), t.getLength());


		ConfirmBuffer cb = new ConfirmBuffer(6);
		cb.addAtomIDs(gm1);
		cb.setMessage(gm3);
		
		Text output = new Text();
		boolean result = cb.load(te, output);
		assertFalse("incorrect tuple",result);
		
		GumboMessageWritable gm4 = new GumboMessageWritable();
		Text t4 = new Text("11,22,11,44");
		gm4.setData(t4.getBytes(), t4.getLength());
		cb.addAtomIDs(gm2);
		cb.setMessage(gm4);
		result = cb.load(te, output);
		assertTrue("correct tuple",result);
		assertEquals("correct output", "22,44,11,44", output.toString());
	}

}
