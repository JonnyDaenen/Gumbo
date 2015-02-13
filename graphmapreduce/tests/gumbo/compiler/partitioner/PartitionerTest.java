/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jonny Daenen
 * 
 */
public class PartitionerTest {

	CalculationUnitGroup cp;

	/**
	 * Dependency setup
	 * 
	 * <pre>
	 *       +------+        +------+                                                            
	 *       |  1   |        |   2  |                                                            
	 * +-----+      |        |      |                                                            
	 * |     ++-----+        +-+----+                                                            
	 * |      |                |                                                                 
	 * |      |                |                                                                 
	 * |      |                |                                                                 
	 * |      |     +-------+  |     +-------+    +------+                                       
	 * |      +----->    3  <--+     |   4   |    |  5   |                                       
	 * |            |       |        |       |    |      |                                       
	 * |            +-------+-----+  +--+----+    +--+---+                                       
	 * |            |             |     |            |                                           
	 * |            |             |     |            |                                           
	 * |            |             |     |            |                                           
	 * |            |             |     |            |                                           
	 * |      +-----v-+       +---v---+ |      +-----v-+      +-------+                                   
	 * |      |       |       |       <-+      |       |      |       |                           
	 * +------>  6    |       |   7   |        |   8   |      |   9   |                     
	 *        +-------+       +-------+        +-------+      +-------+
	 * </pre>
	 */
	@Before
	public void setUp() throws Exception {
		cp = new CalculationUnitGroup();

		GFAtomicExpression guard = new GFAtomicExpression("G", "x");
		GFAtomicExpression output = new GFAtomicExpression("O", "x");
		GFAtomicExpression child = new GFAtomicExpression("R", "x");
		GFExistentialExpression e = new GFExistentialExpression(guard, child, output);

		BasicGFCalculationUnit c1 = new BasicGFCalculationUnit(1, e);
		BasicGFCalculationUnit c2 = new BasicGFCalculationUnit(2, e);
		BasicGFCalculationUnit c3 = new BasicGFCalculationUnit(3, e);
		BasicGFCalculationUnit c4 = new BasicGFCalculationUnit(4, e);
		BasicGFCalculationUnit c5 = new BasicGFCalculationUnit(5, e);
		BasicGFCalculationUnit c6 = new BasicGFCalculationUnit(6, e);
		BasicGFCalculationUnit c7 = new BasicGFCalculationUnit(7, e);
		BasicGFCalculationUnit c8 = new BasicGFCalculationUnit(8, e);
		BasicGFCalculationUnit c9 = new BasicGFCalculationUnit(9, e);

		c1.setDependency(new RelationSchema("Dummy3", "x"), c3);
		c1.setDependency(new RelationSchema("Dummy6", "x"), c6);

		c2.setDependency(new RelationSchema("Dummy3", "x"), c3);

		c3.setDependency(new RelationSchema("Dummy6", "x"), c6);
		c3.setDependency(new RelationSchema("Dummy7", "x"), c7);

		c4.setDependency(new RelationSchema("Dummy7", "x"), c7);

		c5.setDependency(new RelationSchema("Dummy8", "x"), c8);

		// add on new level
		cp.add(c1);
		cp.add(c2);
		cp.add(c3);
		cp.add(c4);
		cp.add(c5);
		cp.add(c6);
		cp.add(c7);
		cp.add(c8);
		cp.add(c9);

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Partition the set based on the height
	 */
	@Test
	public void heightPartitioner() {
		HeightPartitioner partitioner = new HeightPartitioner();

		PartitionedCUGroup partitioned = partitioner.partition(cp,null);
		
		List<CalculationUnitGroup> list = partitioned.getBottomUpList();

		assertEquals(3, list.size());


		//assertEquals(0, list.get(0).size());
		assertEquals(4, list.get(0).size());
		assertEquals(3, list.get(1).size());
		assertEquals(2, list.get(2).size());
		try {
			assertEquals(0, list.get(3).size());
			fail("should throw IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {

		}
	}
}
