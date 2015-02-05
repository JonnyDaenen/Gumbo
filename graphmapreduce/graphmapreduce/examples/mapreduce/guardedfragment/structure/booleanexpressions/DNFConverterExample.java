/**
 * Created: 26 Aug 2014
 */
package mapreduce.guardedfragment.structure.booleanexpressions;

import mapreduce.guardedfragment.structure.conversion.DNFConverter;

/**
 * @author Jonny Daenen
 *
 */
public class DNFConverterExample {
	
	public static void main(String[] args) {
		
		BVariable p1 = new BVariable(1);
		BVariable p2 = new BVariable(2);
		BVariable p3 = new BVariable(3);
		
		BExpression be = new BNotExpression(new BAndExpression(new BOrExpression(p1,p2), new BNotExpression(p3)));
		System.out.println(be);
		
		DNFConverter converter = new DNFConverter();
		System.out.println(converter.convert(be));
	}

}
