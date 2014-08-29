/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.convertors;

import mapreduce.guardedfragment.structure.conversion.DNFConversionException;

/**
 * @author Jonny Daenen
 * 
 */
public class GFConversionException extends Exception {

	public GFConversionException(String msg) {
		super(msg);
	}

	/**
	 * @param e
	 */
	public GFConversionException(DNFConversionException e) {
		super(e);
	}

}
