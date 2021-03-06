package gumbo.structures.conversion;

/**
 * Convert the GFExpression to a boolean expression. A mapping from atomic
 * values to boolean variables is created. Note that identical
 * relations are mapped to the same variable. E.g., B(x) & B(x) is mapped
 * onto v0 & v0. This is the case even when the GFAtomicExpressions are
 * different objects.
 * 
 * @author Jonny Daenen
 */
import gumbo.structures.booleanexpressions.BAndExpression;
import gumbo.structures.booleanexpressions.BEVisitor;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BNotExpression;
import gumbo.structures.booleanexpressions.BOrExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;

public class BooleanToGFConvertor implements BEVisitor<GFExpression> {

	
	GFBooleanMapping mapping;
	
	/**
	 * @param mapping the mapping between boolean variables and GFAtomics
	 */
	public void setMapping(GFBooleanMapping mapping) {
		this.mapping = mapping;
	}

	/**
	 * @param beDNF
	 * @param mapping2
	 * @return
	 */
	public GFExpression convert(BExpression beDNF) {
		return beDNF.accept(this);
	}

	
	@Override
	public GFExpression visit(BExpression e) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public GFExpression visit(BNotExpression e) {
		return new GFNotExpression(convert(e.getChild()));
	}

	@Override
	public GFExpression visit(BAndExpression e) {

		return new GFAndExpression(convert(e.getChild1()),convert(e.getChild2()));
	}

	@Override
	public GFExpression visit(BOrExpression e) {

		return new GFOrExpression(convert(e.getChild1()),convert(e.getChild2()));
	}

	@Override
	public GFExpression visit(BVariable e) {
		// lookup mapping
		GFAtomicExpression var = (GFAtomicExpression) mapping.getAtomic(e);
		// create new atom object
		return new GFAtomicExpression(var);
	}

}
