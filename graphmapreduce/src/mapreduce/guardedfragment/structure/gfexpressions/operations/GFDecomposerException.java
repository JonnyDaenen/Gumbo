/**
 * Created: 28 Apr 2014
 */
package mapreduce.guardedfragment.structure.gfexpressions.operations;

import mapreduce.guardedfragment.structure.gfexpressions.GFVisitorException;

/**
 * @author Jonny Daenen
 *
 */
public class GFDecomposerException extends GFVisitorException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param msg
	 */
	public GFDecomposerException(String msg) {
		super(msg);
	}

}
