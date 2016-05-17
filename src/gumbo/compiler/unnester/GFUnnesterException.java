/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.unnester;

import gumbo.structures.gfexpressions.GFVisitorException;

/**
 * @author Jonny Daenen
 *
 */
public class GFUnnesterException extends GFVisitorException {

	private static final long serialVersionUID = 1L;

	public GFUnnesterException(String msg) {
		super(msg);
	}
	
}
