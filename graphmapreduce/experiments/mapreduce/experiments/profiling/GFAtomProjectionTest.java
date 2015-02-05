/**
 * Created: 17 Sep 2014
 */
package mapreduce.experiments.profiling;

import gumbo.compiler.structures.data.Tuple;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.operations.GFAtomProjection;

/**
 * Testing optimisations:
 * - tuple init: easier pattern and split (12s -> 2.8)
 * - tuple toString: caching (4s -> 0,005s)
 * - tuple toString: StringBuilder (4s -> 1,5s)
 * @author jonny
 * 
 */
public class GFAtomProjectionTest {

	public static void main(String[] args) throws InterruptedException {

		testInit();
		
		// ---
		testMatch();
		
	
	}

	/**
	 * 
	 */
	private static void testInit() {
		long startTime = System.nanoTime();

		GFAtomicExpression a = new GFAtomicExpression("A", "x", "y", "z", "x");
		GFAtomicExpression b = new GFAtomicExpression("B", "x", "y", "z", "x");
		
		for (int i = 0; i < 10000000; i++) {
			
			new GFAtomProjection(a, b);
		}

//		Tuple t = new Tuple("R(100,200,300,400)");
//		System.out.println(t);
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime) / 1000000;
		System.out.println("Projection init: " + duration);
	}

	/**
	 * 
	 */
	private static void testMatch() {
		long startTime = System.nanoTime();

		Tuple t = new Tuple("R(100,200,300,400)");
		GFAtomicExpression a = new GFAtomicExpression("R", "x", "y", "z", "x");
		
		for (int i = 0; i < 10000000; i++) {
			a.matches(t);
		}
		System.out.println(t);
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime) / 1000000;
		System.out.println("Tuple match: " + duration);
	}

}
