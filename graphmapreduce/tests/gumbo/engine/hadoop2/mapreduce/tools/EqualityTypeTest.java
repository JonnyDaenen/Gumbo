package gumbo.engine.hadoop2.mapreduce.tools;

import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import gumbo.engine.hadoop2.mapreduce.tools.tupleops.EqualityType;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class EqualityTypeTest {
	
	
	@Test
	public void testParser() {
		
		GFAtomicExpression atom1 = new GFAtomicExpression("R1", "x","y","x"); 
		
		EqualityType t1 = new EqualityType(atom1);
		
		assertEquals("different atom matches","0,1,0",t1.toString());
		
	}
	
	@Test
	public void testAtomEquality() {
		
		GFAtomicExpression atom1 = new GFAtomicExpression("R1", "x","y","x"); 
		GFAtomicExpression atom2 = new GFAtomicExpression("Q1", "y","x","y"); 
		
		EqualityType t1 = new EqualityType(atom1);
		EqualityType t2 = new EqualityType(atom2);
		
		assertTrue("different atom matches",t1.matches(t2));
		assertTrue("different atom matches",t2.matches(t1));
		assertTrue("different atom equals",t1.equals(t2));
		
	}
	
	
	@Test
	public void testAtomInEquality() {
		
		GFAtomicExpression atom1 = new GFAtomicExpression("R1", "x","y","x"); 
		GFAtomicExpression atom2 = new GFAtomicExpression("Q1", "x","y","z"); 
		
		EqualityType t1 = new EqualityType(atom1);
		EqualityType t2 = new EqualityType(atom2);
		
		assertFalse("different atom matches",t1.matches(t2));
		assertFalse("different atom matches",t2.matches(t1));
		assertFalse("different atom equals",t1.equals(t2));
		
	}
	
	@Test
	public void testTextEquality() {
		
		Text atom1 = new Text("xx,xx,zz"); 
		Text atom2 = new Text("yy,yy,xx"); 
		
		QuickWrappedTuple qt1 = new QuickWrappedTuple();
		QuickWrappedTuple qt2 = new QuickWrappedTuple();
		
		qt1.initialize(atom1);
		qt2.initialize(atom2);
		
		EqualityType t1 = new EqualityType(qt1);
		EqualityType t2 = new EqualityType(qt2);
		
		assertTrue("different atom equals",t1.equals(t2));
		assertTrue("different atom matches",t2.matches(t1));
		assertTrue("different atom equals",t1.equals(t2));
		
	}

}
