package mapreduce.guardedfragment.structure.gfexpressions;



public class GFOrExpression extends GFAndExpression{ // TODO decouple from AND for safety


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
	

	

	
	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}
	
	/**
	 * @see mapreduce.guardedfragment.structure.gfexpressions.GFExpression#containsAnd()
	 */
	@Override
	public boolean containsAnd() {
		return child1.containsAnd() || child2.containsAnd();
	}

}
