package guardedfragment.structure.gfexpressions;


import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.BOrExpression;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;

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
	public String generateString() {
		return "(" + child1.generateString() + " | " + child2.generateString() + ")";
	}
	
	public String prefixString() {
		return "|" + child1.prefixString() + child2.prefixString();
	}
	
	
	@Override
	public BExpression convertToBExpression(GFBooleanMapping m) throws GFtoBooleanConversionException {
		BExpression nc1 = child1.convertToBExpression(m);
		BExpression nc2 = child2.convertToBExpression(m);
		return new BOrExpression(nc1, nc2);
	}
	
	@Override
	public <R> R accept(GFVisitor<R> v) {
		return v.visit(this);
	}
	

}
