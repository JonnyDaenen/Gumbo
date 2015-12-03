package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import gumbo.engine.hadoop2.mapreduce.tools.tupleops.EqualityFilter;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class EqualityFilterTester {

	@Test
	public void correctSizeTest() {
		GFAtomicExpression exp = new GFAtomicExpression("R1", "x1", "x2", "x3", "x2", "x3");
		EqualityFilter ef = new EqualityFilter(exp);

		// correct sizes
		String s1 = "1,2,3,4,5";
		String s2 = "1,2,3,2,5";
		String s3 = "1,2,3,2,3";
		String s4 = "100,100,100,100,100";

		QuickWrappedTuple qt = new QuickWrappedTuple();
		qt.initialize(s1.getBytes(),s1.getBytes().length);
		assertFalse("all different", ef.check(qt));

		qt.initialize(s2.getBytes(),s2.getBytes().length);
		assertFalse("one correct", ef.check(qt));

		qt.initialize(s3.getBytes(),s3.getBytes().length);
		assertTrue("two correct", ef.check(qt));

		qt.initialize(s4.getBytes(),s4.getBytes().length);
		assertTrue("all the same", ef.check(qt));




	}

	@Test
	public void wrongSizeTest() {
		GFAtomicExpression exp = new GFAtomicExpression("R1", "x1", "x2", "x3", "x2", "x3");
		EqualityFilter ef = new EqualityFilter(exp);

		QuickWrappedTuple qt = new QuickWrappedTuple();

		// wrong sizes
		String s5 = "1,1,1,1,1,1";
		String s6 = "1,1,1,1";

		qt.initialize(s5.getBytes(),s5.getBytes().length);
		assertFalse("two correct", ef.check(qt));

		qt.initialize(s6.getBytes(),s6.getBytes().length);
		assertFalse("two correct", ef.check(qt));
	}

}
