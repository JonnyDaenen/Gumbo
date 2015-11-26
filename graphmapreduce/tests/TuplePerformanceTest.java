import gumbo.structures.data.QuickTuple;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

public class TuplePerformanceTest {
	static String name = "R";
	static String s = "1234,123456,123123,5245";
	static String sb = "R(1234,123456,123123,5245)";
	static int NUM_TESTS = 1000000;

	public static void main(String[] args) throws InterruptedException {

		GFAtomProjection pi = new GFAtomProjection(new GFAtomicExpression("R", "x","y","z","w"), new GFAtomicExpression("Out", "w","z","x"));
		
		Thread.sleep(10000);
		System.out.println("Test1");
		test1(pi);
		System.out.println("Test2");
		test2(pi);
		System.out.println("Test3");
		test3(pi);
		System.out.println("Test4");
		test4(pi);
		System.out.println("Test5");
		test5(pi);
		System.out.println("Test6");
		test6(pi);
		
		






	}
	
	
	private static void test6(GFAtomProjection pi) {
		// bytes re-use, no string input
		byte [] b = s.getBytes();
		byte [] b1 = name.getBytes();
		QuickTuple t2 = new QuickTuple(b1,b);
		for (int i = 0; i < NUM_TESTS; i++) {
			t2.initialize(b1, b);
			if (pi != null)
				project(t2, pi);
		}

	}
	
	private static void project(QuickTuple t, GFAtomProjection pi) {
		try {
			pi.projectString(t, true);
		} catch (NonMatchingTupleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private static void test5(GFAtomProjection pi) {
		// bytes re-use, no string input
		Tuple t2 = new Tuple("","");
		byte [] b = s.getBytes();
		for (int i = 0; i < NUM_TESTS; i++) {
			t2.initialize(name, b, b.length);
			if (pi != null)
				project(t2, pi);
		}

	}
	
	private static void test4(GFAtomProjection pi) {
		// bytes re-use, no string input
		Tuple t2 = new Tuple("","");
		byte [] b = sb.getBytes();
		for (int i = 0; i < NUM_TESTS; i++) {
			t2.initialize(b, b.length);
			if (pi != null)
				project(t2, pi);
		}

	}

	private static void test3(GFAtomProjection pi) {
		// bytes re-use
		Tuple t2 = new Tuple("","");
		for (int i = 0; i < NUM_TESTS; i++) {
			byte [] b = sb.getBytes();
			t2.initialize(b, b.length);
			if (pi != null)
				project(t2, pi);
		}

	}

	private static void test2(GFAtomProjection pi) {
		// bytes
		for (int i = 0; i < NUM_TESTS; i++) {
			byte [] b = sb.getBytes();
			Tuple t1 = new Tuple(b,b.length);
			if (pi != null)
				project(t1, pi);

		}
	}

	private static void test1(GFAtomProjection pi) {
		// String
		for (int i = 0; i < NUM_TESTS; i++) {
			Tuple t1 = new Tuple(sb);
			if (pi != null)
				project(t1, pi);
		}
	}


	private static void project(Tuple t1, GFAtomProjection pi) {
		try {
			pi.projectString(t1, true);
		} catch (NonMatchingTupleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
