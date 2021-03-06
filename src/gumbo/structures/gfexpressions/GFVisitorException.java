/**
 * Created: 24 Apr 2014
 */
package gumbo.structures.gfexpressions;

/**
 * Represents an error/exception that arises during visit of GFExpressions.
 * @author Jonny Daenen
 *
 */
public class GFVisitorException extends Exception {


	private static final long serialVersionUID = 1L;
	

	public GFVisitorException(String msg) {
		super(msg);
	}


	/**
	 * @param e1
	 */
	public GFVisitorException(Exception e1) {
		super(e1);
	}


	/**
	 * @param msg
	 * @param e
	 */
	public GFVisitorException(String msg, Exception e) {
		super(msg,e);
	}

}
