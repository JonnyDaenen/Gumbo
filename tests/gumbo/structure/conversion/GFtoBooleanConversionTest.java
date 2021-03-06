package gumbo.structure.conversion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.booleanexpressions.VariableNotFoundException;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.conversion.GFtoBooleanConversionException;
import gumbo.structures.conversion.GFtoBooleanConvertor;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

public class GFtoBooleanConversionTest {

	GFPrefixSerializer serializer;
	GFtoBooleanConvertor convertor;

	@Before
	public void setUp() throws Exception {
		serializer = new GFPrefixSerializer();
		convertor = new GFtoBooleanConvertor();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void atomConversion() throws Exception {
		GFAtomicExpression gfe = (GFAtomicExpression) serializer.deserialize("R(x,y,z,1)");
		BExpression be = convertor.convert(gfe);
		GFBooleanMapping mapping = convertor.getMapping();

		// create different object that is equal
		GFAtomicExpression gfae = new GFAtomicExpression("R", "x", "y", "z", "1");

		BVariable var1 = mapping.getVariable(gfe);
		BVariable var2 = mapping.getVariable(gfae);

		if (!var1.equals(var2))
			fail("mapping does not work for equal atoms (but different objects)");

		BEvaluationContext bec = new BEvaluationContext();

		try {
			boolean n = be.evaluate(bec);
			assertFalse(n);
//			fail("VariableNotFoundException expected");
		} catch (VariableNotFoundException e) {
		}

		bec.setValue(mapping.getVariable(gfae), true);
		assertTrue(be.evaluate(bec));
		
		bec.setValue(mapping.getVariable(gfe), false);
		assertFalse(be.evaluate(bec));

	}

	@Test
	public void bcConversion() throws Exception {
		GFExpression gfe = serializer.deserialize("|&R(x,y)R(y,x)S(x)");
		BExpression be = convertor.convert(gfe);
		GFBooleanMapping mapping = convertor.getMapping();

		// create different object that is equal
		GFAtomicExpression gfae1 = new GFAtomicExpression("R", "x", "y");
		GFAtomicExpression gfae2 = new GFAtomicExpression("R", "y", "x");
		GFAtomicExpression gfae3 = new GFAtomicExpression("S", "x");
		
		BEvaluationContext bec = new BEvaluationContext();
		
		bec.setValue(mapping.getVariable(gfae1), true);
		bec.setValue(mapping.getVariable(gfae3), false);
		
//		System.out.println(be);
		try {
			boolean n = be.evaluate(bec);
			assertFalse(n);
//			fail("VariableNotFoundException expected");
		} catch (VariableNotFoundException e) {
		}
		
		bec.setValue(mapping.getVariable(gfae1), true);
		bec.setValue(mapping.getVariable(gfae2), true);
		bec.setValue(mapping.getVariable(gfae3), false);
		assertTrue(be.evaluate(bec));
		
		bec.setValue(mapping.getVariable(gfae1), true);
		bec.setValue(mapping.getVariable(gfae2), false);
		bec.setValue(mapping.getVariable(gfae3), false);
		assertFalse(be.evaluate(bec));
		
	}

	@Test
	public void bcSameConversion() throws Exception {
		GFExpression gfe = serializer.deserialize("&R(x,y)R(x,y)");
		BExpression be = convertor.convert(gfe);
		GFBooleanMapping mapping = convertor.getMapping();

		// create different object that is equal
		GFAtomicExpression gfae1 = new GFAtomicExpression("R", "x", "y");
		
		BEvaluationContext bec = new BEvaluationContext();
		
		bec.setValue(mapping.getVariable(gfae1), true);
		
//		System.out.println(be);
		try {
			assertTrue(be.evaluate(bec));
		} catch (VariableNotFoundException e) {
			fail("VariableNotFoundException not expected");
		}
		
	}
	
	

	@Test(expected=GFtoBooleanConversionException.class)
	public void wrongConversion() throws Exception {
		GFExpression gfe = serializer.deserialize("#O(x)&R(x,y)S(x)");
		convertor.convert(gfe);
	}
}
