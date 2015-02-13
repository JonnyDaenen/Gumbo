/**
 * Created: 12 May 2014
 */
package gumbo.compiler.resolver;

import gumbo.compiler.GFCompilerException;

/**
 * @author Jonny Daenen
 *
 */
public class UnsupportedCalculationUnitException extends GFCompilerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

	public UnsupportedCalculationUnitException(String msg) {
		super(msg);
	}

}
