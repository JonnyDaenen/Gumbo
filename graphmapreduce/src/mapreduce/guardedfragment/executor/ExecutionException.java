/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.executor;

/**
 * @author jonny
 *
 */
public class ExecutionException extends Exception {

	/**
	 * @param string
	 */
	public ExecutionException(String message, Throwable cause) {
		super(message, cause);
	}
	
	/**
	 * 
	 */
	public ExecutionException(String message) {
		super(message);
	}

}
