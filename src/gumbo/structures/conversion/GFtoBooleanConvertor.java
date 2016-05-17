package gumbo.structures.conversion;

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
import gumbo.structures.booleanexpressions.BAndExpression;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BNotExpression;
import gumbo.structures.booleanexpressions.BOrExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;

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
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public BExpression visit(GFExpression e) throws GFVisitorException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public BExpression visit(GFAtomicExpression e) throws GFVisitorException {
		BVariable v = mapping.getVariable(e);
		return v;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFAndExpression)
	 */
	@Override
	public BExpression visit(GFAndExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BAndExpression(nc1, nc2);
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFOrExpression)
	 */
	@Override
	public BExpression visit(GFOrExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BOrExpression(nc1, nc2);
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFNotExpression)
	 */
	@Override
	public BExpression visit(GFNotExpression e) throws GFVisitorException {
		BExpression nc = convertWithCurrentMapping(e.getChild());
		return new BNotExpression(nc);
	}

	/**
	 * @throws VisitorException
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExistentialExpression)
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
