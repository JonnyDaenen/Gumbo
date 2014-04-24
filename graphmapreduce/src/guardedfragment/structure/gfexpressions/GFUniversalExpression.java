package guardedfragment.structure.gfexpressions;


@Deprecated
public class GFUniversalExpression extends GFExistentialExpression {

	/**
	 * Creates a universal expression, consisting of a guard (atomic expression) 
	 * and a child expression.
	 * @param guard an atomic expression
	 * @param child a child expression
	 * @param variables the quantified variables
	 */
	public GFUniversalExpression(GFAtomicExpression guard, GFExpression child, String name, String[] variables) {
		super(guard, child, name, variables);
		super.quantifierSymbol = 'A';
	}


	
	@Override
	public <R> R accept(GFVisitor<R> v) throws GFVisitorException {
		return v.visit(this);
	}

}
