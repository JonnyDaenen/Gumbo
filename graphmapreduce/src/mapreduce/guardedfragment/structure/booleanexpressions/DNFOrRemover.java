/**
 * Created: 26 Aug 2014
 */
package mapreduce.guardedfragment.structure.booleanexpressions;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Jonny Daenen
 * 
 */
public class DNFOrRemover implements BEVisitor<Iterable<BExpression>> {


	private Set<BExpression> newSet(BExpression e) {
		HashSet<BExpression> set = new HashSet<BExpression>();
		set.add(e);
		return set;
	}
	
	/**
	 * Removes the top-level OR statemtents and returns the non-or statements separately. 
	 * @param e a boolean expression
	 * @return an iterable over the non-or statments
	 */
	public Iterable<BExpression> extractTopLevel(BExpression e) {
		return e.accept(this);
	}

	/**
	 * @see mapreduce.guardedfragment.structure.booleanexpressions.BEVisitor#visit(mapreduce.guardedfragment.structure.booleanexpressions.BExpression)
	 */
	@Override
	public Iterable<BExpression> visit(BExpression e) {
		return newSet(e);
	}

	/**
	 * @see mapreduce.guardedfragment.structure.booleanexpressions.BEVisitor#visit(mapreduce.guardedfragment.structure.booleanexpressions.BNotExpression)
	 */
	@Override
	public Iterable<BExpression> visit(BNotExpression e) {
		return newSet(e);
	}

	/**
	 * @see mapreduce.guardedfragment.structure.booleanexpressions.BEVisitor#visit(mapreduce.guardedfragment.structure.booleanexpressions.BAndExpression)
	 */
	@Override
	public Iterable<BExpression> visit(BAndExpression e) {
		return newSet(e);
	}

	/**
	 * @see mapreduce.guardedfragment.structure.booleanexpressions.BEVisitor#visit(mapreduce.guardedfragment.structure.booleanexpressions.BOrExpression)
	 */
	@Override
	public Iterable<BExpression> visit(BOrExpression e) {
		Iterable<BExpression> t1 = extractTopLevel(e.getChild1());
		Iterable<BExpression> t2 = extractTopLevel(e.getChild2());
		
		HashSet<BExpression> result = new HashSet<BExpression>();
		for (BExpression bExpression : t1) {
			result.add(bExpression);
		}
		
		for (BExpression bExpression : t2) {
			result.add(bExpression);
		}
		
		return result;
	}

	/**
	 * @see mapreduce.guardedfragment.structure.booleanexpressions.BEVisitor#visit(mapreduce.guardedfragment.structure.booleanexpressions.BVariable)
	 */
	@Override
	public Iterable<BExpression> visit(BVariable e) {
		// TODO Auto-generated method stub
		return null;
	}

}
