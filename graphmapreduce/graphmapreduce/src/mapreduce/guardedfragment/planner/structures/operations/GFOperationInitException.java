/**
 * Created: 25 Sep 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

/**
 * @author Jonny Daenen
 *
 */
public class GFOperationInitException extends Exception {
	


	private static final long serialVersionUID = 1L;


	public GFOperationInitException(String msg) {
		super(msg);
	}
	

	public GFOperationInitException(Exception e) {
		super(e);
	}

}
