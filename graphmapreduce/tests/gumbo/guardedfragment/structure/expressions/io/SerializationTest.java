/**
 * Created: 23 Apr 2014
 */
package gumbo.guardedfragment.structure.expressions.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jonny Daenen
 * 
 */
public class SerializationTest {

	GFExpression atom;
	GFExpression atomAND;
	GFExpression atomBC;
	GFExpression basic;
	GFExpression basicBC;
	GFExpression rank2;

	GFExpression rank2BC;
	GFExpression doubleRef; // 2 same atom objects

	GFPrefixSerializer prefixSerializer;

	/**
	 * Set up different types of expressions.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {

		// serializers
		prefixSerializer = new GFPrefixSerializer();

		// atom
		atom = new GFAtomicExpression("R", "x", "y", "x");

		// Boolean combination of atoms
		GFExpression atomBC1 = new GFAtomicExpression("R", "x", "y", "x");
		GFExpression atomBC2 = new GFAtomicExpression("S", "z","s");

		atomAND = new GFAndExpression(atomBC1,atomBC2);
		atomBC = new GFAndExpression(atomAND, new GFOrExpression(atomBC2, new GFNotExpression(atomBC1)));

		// rank 1, no boolean combination
		GFAtomicExpression guard = new GFAtomicExpression("R", "x", "y", "x");
		GFAtomicExpression guarded = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression output = new GFAtomicExpression("O", "x");
		basic = new GFExistentialExpression(guard, guarded, output);

		// rank 1, with boolean combination
		GFAtomicExpression guard1 = new GFAtomicExpression("R", "x", "y", "x", "z");
		GFAtomicExpression guarded1 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression guarded2 = new GFAtomicExpression("T", "y", "z");
		GFAtomicExpression output1 = new GFAtomicExpression("O", "x", "z");

		GFExpression bc1 = new GFNotExpression(guarded1);
		GFExpression bc2 = new GFAndExpression(guarded2, bc1);
		basicBC = new GFExistentialExpression(guard1, bc2, output1);

		// Rank 2 no BC
		GFAtomicExpression guardR22 = new GFAtomicExpression("R", "x", "y", "x", "z");
		GFAtomicExpression guardR21 = new GFAtomicExpression("S", "x", "y");
		GFAtomicExpression guardedR22 = new GFAtomicExpression("T", "y");
		GFAtomicExpression outputR21 = new GFAtomicExpression("O1", "x");
		GFAtomicExpression outputR22 = new GFAtomicExpression("O2", "x", "z");

		GFExpression rank21 = new GFExistentialExpression(guardR21, guardedR22, outputR21);
		rank2 = new GFExistentialExpression(guardR22, rank21, outputR22);

	}

	@After
	public void tearDown() throws Exception {

	}

	// --- PREFIX SERIALIZATION TESTS

	@Test
	public void serializeAtom() {
		test2way(atom, "R(x,y,x)");
	}

	@Test
	public void serializeAtomAnd() {
		test2way(atomAND, "&R(x,y,x)S(z,s)");
	}
	
	@Test
	public void serializeAtomBC() {
		test2way(atomBC, "&&R(x,y,x)S(z,s)|S(z,s)!R(x,y,x)");
	}

	@Test
	public void serializeBasic() {
		test2way(basic, "#O(x)&R(x,y,x)S(x,y)"); 
	}
	
	@Test
	public void serializeBasicBC() {
		test2way(basicBC, "#O(x,z)&R(x,y,x,z)&T(y,z)!S(x,y)"); 
	}

	@Test
	public void serializeRank2() {
		test2way(rank2, "#O2(x,z)&R(x,y,x,z)#O1(x)&S(x,y)T(y)"); 
	}

	private void test2way(GFExpression e, String representation) {
		
		// test representation
		String ser = prefixSerializer.serialize(e);
		assertEquals(representation,ser);
//		System.out.println(representation);
//		System.out.println(ser);

		// 2-way test
		try {
			String ser2 = prefixSerializer.serialize(prefixSerializer.deserialize(ser));
//			System.out.println(ser2);
			assertEquals(ser, ser2);
		} catch(DeserializeException ex) {
			fail("Exception thrown: " + ex.getMessage());
		}
	}
	
	// --- SETS
	
	@Test
	public void sets() throws DeserializeException{
		HashSet<GFExpression> strings= new HashSet<GFExpression>();
		strings.add(atom);
		strings.add(atomAND);
		strings.add(atomBC);
		strings.add(basic);
		strings.add(basicBC);
		strings.add(rank2);
		
		String ser = prefixSerializer.serializeSet(strings);
		Set<GFExpression> strings2 = prefixSerializer.deserializeSet(ser);
		
		for (GFExpression e : strings) {
			boolean found = false;
			for (GFExpression e2 : strings2) {
				if(e.toString().equals(e2.toString())) { // TODO implement equals
					found = true;
				}
			}
			assertEquals(true, found);
		}
		
		for (GFExpression e : strings2) {
			boolean found = false;
			for (GFExpression e2 : strings) {
				if(e.toString().equals(e2.toString())) { // TODO implement equals
					found = true;
				}
			}
			assertEquals(true, found);
		}
	}
	
	
	// -- DESERIALIZATION EXCEPTIONS
	@Test(expected=DeserializeException.class) 
	public void IncompleteException1() throws DeserializeException{
		prefixSerializer.deserialize("R");
	}
	
	@Test(expected=DeserializeException.class) 
	public void IncompleteException2() throws DeserializeException{
		prefixSerializer.deserialize("R(");
	}
	
	@Test(expected=DeserializeException.class) 
	public void IncompleteException3() throws DeserializeException{
		prefixSerializer.deserialize("R(x");
	}
	
	@Test(expected=DeserializeException.class) 
	public void IncompleteException4() throws DeserializeException{
		prefixSerializer.deserialize("R(x,");
	}
	
	@Test(expected=DeserializeException.class) 
	public void IncompleteException5() throws DeserializeException{
		prefixSerializer.deserialize("#O(x)&R(x,y)");
	}

	@Test(expected=DeserializeException.class) 
	public void IncompleteException6() throws DeserializeException{
		prefixSerializer.deserialize("#O(x)&R(x,y)");
	}
	
	@Test(expected=DeserializeException.class) 
	public void EmptyException() throws DeserializeException{
		prefixSerializer.deserialize("  ");
	}
	
	@Test(expected=DeserializeException.class) 
	public void NonAtomicException() throws DeserializeException{
		prefixSerializer.deserialize("#&O(x,y)&R(x,y)&S(x,y)");
	}
	
	@Test(expected=DeserializeException.class) 
	public void wrongGuardException() throws DeserializeException{
		prefixSerializer.deserialize("#O(x,y)|R(x,y)&S(x,y)");
	}
	
	@Test(expected=DeserializeException.class) 
	public void BCGuardException() throws DeserializeException{
		prefixSerializer.deserialize("#O(x,y)&&R(x,y)T(x)&S(x,y)R(x,y)");
	}
	
	@Test(expected=DeserializeException.class) 
	public void existentialGuardException() throws DeserializeException{
		prefixSerializer.deserialize("#O(x,y)&#O1(x)&R(x,y)S(x)&T(x)S(x,y)");
	}
	
	@Test(expected=DeserializeException.class) 
	public void relationNotationException() throws DeserializeException{
		prefixSerializer.deserialize("&R(x,y)S)x,y("); 
	}
	
	@Test(expected=DeserializeException.class) 
	public void relationNameException() throws DeserializeException{
		prefixSerializer.deserialize("l?(x,y)"); 
	}
	
	@Test
	public void noException() throws DeserializeException{
		prefixSerializer.deserialize("#O(x,y)&R(x,y)&T(x)|S(x,y)T(x)");
	}
	
	
	
	
	
}
