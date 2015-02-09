package gumbo.compiler;

import gumbo.guardedfragment.gfexpressions.GFVisitorException;

public class GFCompilerException extends Exception {

	private static final long serialVersionUID = 1L;
	
	
	public GFCompilerException(String msg) {
		super(msg);
	}
	
	public GFCompilerException(String msg, Exception e) {
		super(msg,e);
	}


}
