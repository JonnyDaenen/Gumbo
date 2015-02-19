/**
 * Created: 25 Aug 2014
 */
package gumbo.convertors;


/**
 * @author Jonny Daenen
 * 
 */
public class GFConversionException extends Exception {

	private static final long serialVersionUID = 1L;

	public GFConversionException(String msg) {
		super(msg);
	}

	/**
	 * @param e
	 */
	public GFConversionException(String msg, Exception e) {
		super(msg, e);
	}

}
