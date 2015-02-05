package gumbo.compiler;

import gumbo.guardedfragment.gfexpressions.GFVisitorException;

public class GFMRPlannerException extends GFVisitorException {

	private static final long serialVersionUID = 1L;
	
	public GFMRPlannerException() {
		this("Not implemented.");
	}
	
	public GFMRPlannerException(String msg) {
		super(msg);
	}

	/**
	 * @param e1
	 */
	public GFMRPlannerException(Exception e1) {
		super(e1);
	}

}
