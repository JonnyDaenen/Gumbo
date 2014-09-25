/**
 * Created: 17 Sep 2014
 */
package mapreduce.experiments.profiling;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.planner.structures.data.Tuple;

/**
 * Testing optimisations:
 * - tuple init: easier pattern and split (12s -> 2.8)
 * - tuple toString: caching (4s -> 0,005s)
 * - tuple toString: StringBuilder (4s -> 1,5s)
 * @author jonny
 * 
 */
public class Tuple_test {

	public static void main(String[] args) {

		testInit();
		
		// ---
		testToString();
		
	
	}

	/**
	 * 
	 */
	private static void testInit() {
		long startTime = System.nanoTime();

		for (int i = 0; i < 10000000; i++) {
			new Tuple("R(100,200,300,400)");
		}

//		Tuple t = new Tuple("R(100,200,300,400)");
//		System.out.println(t);
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime) / 1000000;
		System.out.println("Tuple init: " + duration);
	}

	/**
	 * 
	 */
	private static void testToString() {
		long startTime = System.nanoTime();

		Tuple t = new Tuple("R(100,200,300,400)");
		for (int i = 0; i < 10000000; i++) {
			t.toString();
		}
		System.out.println(t);
		
		long endTime = System.nanoTime();

		long duration = (endTime - startTime) / 1000000;
		System.out.println("Tuple toString: " + duration);
	}

}
