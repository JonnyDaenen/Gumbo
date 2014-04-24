/**
 * Created: 24 Apr 2014
 */
package guardedfragment.structure.gfexpressions;

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

}
