package gumbo.guardedfragment.conversion;

/**
 * Convert the GFExpression to a boolean expression. A mapping from atomic
 * values to boolean variables is created and reused for all future conversions, unless it is cleared.
 * Note that identical
 * relations are mapped to the same variable. E.g., B(x) & B(x) is mapped
 * onto v0 & v0. This is the case even when the GFAtomicExpressions are
 * different objects.
 * 
 * @author Jonny Daenen
 */
import gumbo.guardedfragment.booleanexpressions.BAndExpression;
import gumbo.guardedfragment.booleanexpressions.BExpression;
import gumbo.guardedfragment.booleanexpressions.BNotExpression;
import gumbo.guardedfragment.booleanexpressions.BOrExpression;
import gumbo.guardedfragment.booleanexpressions.BVariable;
import gumbo.guardedfragment.gfexpressions.GFAndExpression;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.GFNotExpression;
import gumbo.guardedfragment.gfexpressions.GFOrExpression;
import gumbo.guardedfragment.gfexpressions.GFVisitor;
import gumbo.guardedfragment.gfexpressions.GFVisitorException;

public class GFtoBooleanConvertor implements GFVisitor<BExpression> {

	GFBooleanMapping mapping;
	
	/**
	 * 
	 */
	public GFtoBooleanConvertor() {
		clearMapping();
	}
	
	/**
	 * sets a new empty mapping to use. 
	 */
	public void clearMapping() {
		mapping = new GFBooleanMapping();
	}

	public BExpression convert(GFExpression gfe) throws GFtoBooleanConversionException {
		
		return convertWithCurrentMapping(gfe);

	}

	private BExpression convertWithCurrentMapping(GFExpression gfe) throws GFtoBooleanConversionException {
		try {
			return gfe.accept(this);
		} catch (GFVisitorException e) {
			throw new GFtoBooleanConversionException(e.getMessage());
		}
	}

	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFExpression)
	 */
	@Override
	public BExpression visit(GFExpression e) throws GFVisitorException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public BExpression visit(GFAtomicExpression e) throws GFVisitorException {
		BVariable v = mapping.getVariable(e);
		return v;
	}

	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFAndExpression)
	 */
	@Override
	public BExpression visit(GFAndExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BAndExpression(nc1, nc2);
	}

	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFOrExpression)
	 */
	@Override
	public BExpression visit(GFOrExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BOrExpression(nc1, nc2);
	}

	/**
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFNotExpression)
	 */
	@Override
	public BExpression visit(GFNotExpression e) throws GFVisitorException {
		BExpression nc = convertWithCurrentMapping(e.getChild());
		return new BNotExpression(nc);
	}

	/**
	 * @throws VisitorException
	 * @see gumbo.guardedfragment.gfexpressions.GFVisitor#visit(gumbo.guardedfragment.gfexpressions.GFExistentialExpression)
	 */
	@Override
	public BExpression visit(GFExistentialExpression e) throws GFVisitorException {
		throw new GFVisitorException(
				"It's not possible to convert formulas that are not a boolean combination of atomic formula's.");
	}

	/**
	 * @return the mapping
	 */
	public GFBooleanMapping getMapping() {
		return mapping;
	}

}
