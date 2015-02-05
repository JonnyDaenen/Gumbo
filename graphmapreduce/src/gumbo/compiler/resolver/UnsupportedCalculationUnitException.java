/**
 * Created: 12 May 2014
 */
package gumbo.compiler.resolver;

import gumbo.compiler.GFMRPlannerException;

/**
 * @author Jonny Daenen
 *
 */
public class UnsupportedCalculationUnitException extends GFMRPlannerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

	public UnsupportedCalculationUnitException(String msg) {
		super(msg);
	}

}
