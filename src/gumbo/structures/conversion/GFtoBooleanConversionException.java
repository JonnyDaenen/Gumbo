package gumbo.structures.conversion;

import gumbo.structures.gfexpressions.GFVisitorException;

/**
 * Exception to be used in converting GFExpression to BExpressions.
 * @author Jonny Daenen
 *
 */
public class GFtoBooleanConversionException extends GFVisitorException {

	private static final long serialVersionUID = 1L;

	public GFtoBooleanConversionException(String msg) {
		super(msg);
	}
	
	

}
