/**
 * Created: 26 Aug 2014
 */
package gumbo.structures.conversion;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.structures.booleanexpressions.BAndExpression;
import gumbo.structures.booleanexpressions.BEVisitor;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BNotExpression;
import gumbo.structures.booleanexpressions.BOrExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;

/**
 * based on pseudo-code from: http://www.cs.jhu.edu/~jason/tutorials/convert-to-CNF.html
 * @author Jonny
 * 
 * 
 * 
 */
public class DNFConverter implements BEVisitor<BExpression> {

	public class DNFConversionException extends Exception {
		
		private static final long serialVersionUID = 1L;

		public DNFConversionException(Throwable e) {
			super(e);
		}

	}

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(DNFConverter.class); 

	DNFOrRemover orRemover;
	GFtoBooleanConvertor boolConverter;
	BooleanToGFConvertor gfConverter;

	public DNFConverter() {
		orRemover = new DNFOrRemover();
		boolConverter = new GFtoBooleanConvertor();
		gfConverter = new BooleanToGFConvertor();
	}

	/**
	 * Converts a basic GF to DNF
	 * @param e
	 * @return
	 * @throws DNFConversionException 
	 * 
	 * @pre e is a basic {@link GFExpression}
	 */
	public GFExpression convert(GFExpression e) throws DNFConversionException {

		if(!e.isBasicGF()){
			;
			// TODO throw excp.
		}


		try {
			GFExistentialExpression gfee = (GFExistentialExpression) e;

			// extract boolean part
			BExpression be = boolConverter.convert(gfee.getChild());
			GFBooleanMapping mapping = boolConverter.getMapping();

			// convert to DNF
			BExpression beDNF = convert(be);

			// re-assemble using variable/atomic mapping
			gfConverter.setMapping(mapping);
			GFExpression dnfChild = gfConverter.convert(beDNF);

			return new GFExistentialExpression(gfee.getGuard(), dnfChild, gfee.getOutputRelation());




		} catch (GFtoBooleanConversionException e1) {
			throw new DNFConversionException(e1);
		}



	}

	public BExpression convert(BExpression e) {

		return e.accept(this);
	}

	/**
	 * @see gumbo.structures.booleanexpressions.BEVisitor#visit(gumbo.structures.booleanexpressions.BNotExpression)
	 */
	@Override
	public BExpression visit(BNotExpression e) {
		BExpression c = e.getChild();

		// double negation
		if (c instanceof BNotExpression)

			return convert(((BNotExpression) c).getChild());

		// de morgan 1: push negation in OR
		else if (c instanceof BOrExpression) {
			BExpression orChild1 = ((BOrExpression) c).getChild1();
			BExpression orChild2 = ((BOrExpression) c).getChild2();
			// push negation inwards
			BExpression newChild1 = new BNotExpression(orChild1);
			BExpression newChild2 = new BNotExpression(orChild2);
			BExpression newAnd = new BAndExpression(newChild1, newChild2);

			return convert(newAnd);
		}
		// de morgan 2: push negation in AND
		else if (c instanceof BAndExpression) {
			BExpression orChild1 = ((BAndExpression) c).getChild1();
			BExpression orChild2 = ((BAndExpression) c).getChild2();
			// push negation inwards
			BExpression newChild1 = new BNotExpression(orChild1);
			BExpression newChild2 = new BNotExpression(orChild2);
			BExpression newOr = new BOrExpression(newChild1, newChild2);

			return convert(newOr);
		} else if(c instanceof BVariable)
			return new BNotExpression(convert(c));

		// TODO throw exception: unsupported type
		return null;
	}

	/**
	 * @see gumbo.structures.booleanexpressions.BEVisitor#visit(gumbo.structures.booleanexpressions.BAndExpression)
	 */
	@Override
	public BExpression visit(BAndExpression e) {



		// convert children
		BExpression c1 = convert(e.getChild1());
		BExpression c2 = convert(e.getChild2());
		Iterable<BExpression> set1 = orRemover.extractTopLevel(c1);
		Iterable<BExpression> set2 = orRemover.extractTopLevel(c2);


		// pairwise combine
		Set<BAndExpression> andSet = new HashSet<>();
		for(BExpression p : set1){
			for(BExpression q : set2){
				andSet.add(new BAndExpression(p,q));
			}
		}

		// 0 cannot happen
		if (andSet.size() < 2)
			return andSet.iterator().next();

		BAndExpression [] array = andSet.toArray(new BAndExpression[0]);


		// add disjunction between the AND-statements
		BOrExpression result = new BOrExpression(array[0], array[1]);
		for (int i = 2; i < array.length; i++) {
			BAndExpression ae = array[i];
			result = new BOrExpression(result,ae);
		}

		return result;
	}


	/**
	 * @see gumbo.structures.booleanexpressions.BEVisitor#visit(gumbo.structures.booleanexpressions.BOrExpression)
	 */
	@Override
	public BExpression visit(BOrExpression e) {

		BExpression c1 = convert(e.getChild1());
		BExpression c2 = convert(e.getChild2());

		return new BOrExpression(c1, c2);
	}

	/**
	 * @see gumbo.structures.booleanexpressions.BEVisitor#visit(gumbo.structures.booleanexpressions.BVariable)
	 */
	@Override
	public BExpression visit(BVariable e) {
		// just return the variable itself (equal object)
		return new BVariable(e);
	}

	/**
	 * @see gumbo.structures.booleanexpressions.BEVisitor#visit(gumbo.structures.booleanexpressions.BExpression)
	 */
	@Override
	public BExpression visit(BExpression e) {
		// TODO unsupported type
		return null;
	}

}
