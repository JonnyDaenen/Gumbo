package gumbo.convertors;

import gumbo.input.GumboQuery;

/**
 * @author brentchesny
 *
 */
public interface GFConverter {
	
	public String convert(GumboQuery query) throws GFConversionException;
	
}
