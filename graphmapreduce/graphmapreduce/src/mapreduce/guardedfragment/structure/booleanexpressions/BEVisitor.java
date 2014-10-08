/**
 * Created: 26 Aug 2014
 */
package mapreduce.guardedfragment.structure.booleanexpressions;

/**
 * @author Jonny Daenen
 *
 */
public interface BEVisitor<T> {

	T visit(BExpression e);
	T visit(BNotExpression e);
	T visit(BAndExpression e);
	T visit(BOrExpression e);
	T visit(BVariable e);
	
	
}