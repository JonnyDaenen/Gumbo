package guardedfragment.mapreduce.planner;

import guardedfragment.structure.gfexpressions.GFVisitorException;

public class GFMRPlannerException extends GFVisitorException {

	private static final long serialVersionUID = 1L;
	
	public GFMRPlannerException() {
		this("Not implemented.");
	}
	
	public GFMRPlannerException(String msg) {
		super(msg);
	}

}
