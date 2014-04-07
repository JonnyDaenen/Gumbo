package guardedfragment.structure;

import guardedfragment.booleanstructure.BExpression;
import guardedfragment.booleanstructure.BOrExpression;

public class GFOrExpression extends GFAndExpression{


	/**
	 * An OR-expression in the Guarded Fragment.
	 * @param c1 first child
	 * @param c2 second child
	 */
	public GFOrExpression(GFExpression c1, GFExpression c2) {
		super(c1,c2);
		rank = Math.max(c1.getRank(),c2.getRank());
	}
	
	
	@Override
	/**
	 * Evaluates this subtree in the given context by evaluating both children and combining the result.
	 * 
	 * @return true if at least one child evaluates to true, false otherwise
	 */
	public boolean evaluate(GFEvaluationContext c) {

		return child1.evaluate(c) || child2.evaluate(c);
	}
	
	@Override
	public String generateString() {
		return "(" + child1.generateString() + " | " + child2.generateString() + ")";
	}
	
	public String prefixString() {
		return "|" + child1.prefixString() + child2.prefixString();
	}
	
	
	@Override
	public BExpression convertToBExpression(GFBMapping m) throws GFConversionException {
		BExpression nc1 = child1.convertToBExpression(m);
		BExpression nc2 = child2.convertToBExpression(m);
		return new BOrExpression(nc1, nc2);
	}
	

}
