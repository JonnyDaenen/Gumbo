/**
 * Created: 11 Apr 2014
 */
package mapreduce.guardedfragment.structure.gfexpressions;


/**
 * @author Jonny Daenen
 *
 */
public interface GFVisitor<R> {
	
	abstract R visit(GFExpression e) throws GFVisitorException;
	
	abstract R visit(GFAtomicExpression e) throws GFVisitorException;
	
	abstract R visit(GFAndExpression e) throws GFVisitorException;
	abstract R visit(GFOrExpression e) throws GFVisitorException;
	abstract R visit(GFNotExpression e) throws GFVisitorException;
	
	abstract R visit(GFExistentialExpression e) throws GFVisitorException;
	
}

