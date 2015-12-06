/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.unnester;

import gumbo.structures.gfexpressions.GFVisitorException;

/**
 * TODO move to the GFDecomposer class
 * @author Jonny Daenen
 *
 */
public class GFDecomposerException extends GFVisitorException {

	private static final long serialVersionUID = 1L;

	public GFDecomposerException(String msg) {
		super(msg);
	}
	
}
