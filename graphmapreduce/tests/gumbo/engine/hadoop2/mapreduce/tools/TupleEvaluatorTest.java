package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import gumbo.engine.hadoop2.mapreduce.tools.tupleops.TupleEvaluator;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class TupleEvaluatorTest {
	
	@Test
	public void evaluatorTest() {
		
		GFAtomicExpression guard = new GFAtomicExpression("R", "x", "y", "x", "z");
		GFAtomicExpression out = new GFAtomicExpression("O", "y", "z", "x", "z");
		
		GFAtomicExpression guarded1 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression guarded2 = new GFAtomicExpression("T", "x", "x");
		
		GFAndExpression child = new GFAndExpression(guarded1, guarded2);
		
		GFExistentialExpression e = new GFExistentialExpression(guard, child, out);
		
		Map<String, Integer> atomids;
		atomids = new HashMap<String, Integer>();
		atomids.put(guard.toString(), 1);
		atomids.put(guarded1.toString(), 4);
		atomids.put(guarded2.toString(), 6);
		atomids.put(out.toString(), 5);
		
		TupleEvaluator te = new TupleEvaluator(e, "outfile.txt", atomids);
		
		boolean [] context = {false, false, false, false, true, false, true, false};
		boolean result = te.eval(context);
		assertTrue("true case", result);
		
		boolean [] context2 = {false, false, false, false, true, false, false, false};
		result = te.eval(context2);
		assertFalse("false case", result);
		
		boolean [] context3 = {false, false, false, false, false, false, true, false};
		result = te.eval(context3);
		assertFalse("false case", result);
		
		boolean [] context4 = {false, false, false, false, false, false, false, false};
		result = te.eval(context4);
		assertFalse("false case", result);
		
		boolean [] context5 = {true, true, true, true, false, true, false, true};
		result = te.eval(context5);
		assertFalse("false case", result);
		
		// projecting 
		QuickWrappedTuple qt = new QuickWrappedTuple();
		Text output = new Text();
		qt.initialize(new Text("1,2,4,5"));
		result = te.project(qt, output, context);
		assertFalse("bad tuple", result);
		

		qt.initialize(new Text("1,2,1,3"));
		result = te.project(qt, output, context);
		assertTrue("good tuple", result);
		assertEquals("text output", "2,3,1,3", output.toString());
	}

}
