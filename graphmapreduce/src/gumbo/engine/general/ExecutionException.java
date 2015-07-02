/**
 * Created: 25 Aug 2014
 */
package gumbo.engine.general;

/**
 * @author Jonny Daenen
 *
 */
public class ExecutionException extends Exception {

	private static final long serialVersionUID = 1L;

	
	public ExecutionException(String message, Throwable cause) {
		super(message, cause);
	}
	

	public ExecutionException(String message) {
		super(message);
	}

}
