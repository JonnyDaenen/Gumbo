package gumbo.compiler;

import gumbo.guardedfragment.gfexpressions.GFVisitorException;

public class GFCompilerException extends GFVisitorException {

	private static final long serialVersionUID = 1L;
	
	public GFCompilerException() {
		this("Not implemented.");
	}
	
	public GFCompilerException(String msg) {
		super(msg);
	}

	/**
	 * @param e1
	 */
	public GFCompilerException(Exception e1) {
		super(e1);
	}

}
