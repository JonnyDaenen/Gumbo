/**
 * 
 */
package tests;

/**
 * @author Jonny Daenen
 *
 */
public class ExceptionTest {


	public class Exception1 extends Exception {

		private static final long serialVersionUID = 1L;

		Exception1 (String msg, Exception e) {
			super(msg, e);
		}

		public Exception1(String msg) {
			super(msg);
		}
	}

	public class Exception2 extends Exception {

		private static final long serialVersionUID = 1L;

		Exception2 (String msg, Exception e) {
			super(msg, e);
		}
	}

	public static void main(String [] args) {

		try {
			ExceptionTest e = new ExceptionTest(); 
			e.test();

		} catch (Exception e ) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

	}

	private void test() throws Exception2 {


		try {
			test1();

		} catch (Exception e ) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			throw new Exception2("normal error", e);
//			throw new Exception2("normal error: " + e.getMessage(), e);
//			throw new Exception2("normal error: " + e.getLocalizedMessage(), e);
		}

	}

	private void test1() throws Exception1 {

		throw new Exception1("deep error");

	}


}
