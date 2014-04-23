/**
 * Created: 11 Apr 2014
 */
package guardedfragment.structure.expressions;


/**
 * @author Jonny Daenen
 *
 */
public interface GFVisitor<R> {
	
	abstract R visit(GFExpression e);
	
	abstract R visit(GFAtomicExpression e);
	
	abstract R visit(GFAndExpression e);
	abstract R visit(GFOrExpression e);
	abstract R visit(GFNotExpression e);
	
	abstract R visit(GFExistentialExpression e);
	abstract R visit(GFUniversalExpression e);
	
}

