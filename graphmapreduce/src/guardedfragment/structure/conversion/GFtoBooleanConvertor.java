package guardedfragment.structure.conversion;

/**
 * Convert the GFExpression to a boolean expression. A mapping from atomic
 * values to boolean variables is created. Note that identical
 * relations are mapped to the same variable. E.g., B(x) & B(x) is mapped
 * onto v0 & v0. This is the case even when the GFAtomicExpressions are
 * different objects.
 * 
 * @author Jonny Daenen
 */
import guardedfragment.structure.booleanexpressions.BAndExpression;
import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.BNotExpression;
import guardedfragment.structure.booleanexpressions.BOrExpression;
import guardedfragment.structure.booleanexpressions.BVariable;
import guardedfragment.structure.gfexpressions.GFAndExpression;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.GFNotExpression;
import guardedfragment.structure.gfexpressions.GFOrExpression;
import guardedfragment.structure.gfexpressions.GFUniversalExpression;
import guardedfragment.structure.gfexpressions.GFVisitor;
import guardedfragment.structure.gfexpressions.GFVisitorException;

public class GFtoBooleanConvertor implements GFVisitor<BExpression> {

	GFBooleanMapping mapping;

	public BExpression convert(GFExpression gfe) throws GFtoBooleanConversionException {
		mapping = new GFBooleanMapping();
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
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFExpression)
	 */
	@Override
	public BExpression visit(GFExpression e) throws GFVisitorException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public BExpression visit(GFAtomicExpression e) throws GFVisitorException {
		BVariable v = mapping.getVariable(e);
		return v;
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFAndExpression)
	 */
	@Override
	public BExpression visit(GFAndExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BAndExpression(nc1, nc2);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFOrExpression)
	 */
	@Override
	public BExpression visit(GFOrExpression e) throws GFVisitorException {
		BExpression nc1 = convertWithCurrentMapping(e.getChild1());
		BExpression nc2 = convertWithCurrentMapping(e.getChild2());
		return new BOrExpression(nc1, nc2);
	}

	/**
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFNotExpression)
	 */
	@Override
	public BExpression visit(GFNotExpression e) throws GFVisitorException {
		BExpression nc = convertWithCurrentMapping(e.getChild());
		return new BNotExpression(nc);
	}

	/**
	 * @throws VisitorException
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFExistentialExpression)
	 */
	@Override
	public BExpression visit(GFExistentialExpression e) throws GFVisitorException {
		throw new GFVisitorException(
				"It's not possible to convert formulas that are not a boolean combination of atomic formula's.");
	}

	/**
	 * @throws GFVisitorException
	 * @see guardedfragment.structure.gfexpressions.GFVisitor#visit(guardedfragment.structure.gfexpressions.GFUniversalExpression)
	 */
	@Override
	public BExpression visit(GFUniversalExpression e) throws GFVisitorException {
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
