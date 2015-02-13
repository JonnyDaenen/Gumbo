/**
 * Created: 26 Aug 2014
 */
package mapreduce.guardedfragment.structure;

import gumbo.structures.conversion.DNFConversionException;
import gumbo.structures.conversion.DNFConverter;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;

/**
 * @author Jonny Daenen
 *
 */
public class DNFConverterExample {
	
	public static void main(String[] args) throws DNFConversionException {
		
		GFAtomicExpression R = new GFAtomicExpression("R","x1","x2","y1","y2","y3");
		GFAtomicExpression S = new GFAtomicExpression("S","x1","y1");
		GFAtomicExpression T = new GFAtomicExpression("T","y1","y2");
		GFAtomicExpression U = new GFAtomicExpression("U","y2","y3");
		GFAtomicExpression V = new GFAtomicExpression("V","y3","x2");
		GFAtomicExpression W = new GFAtomicExpression("S","x1","y1");
		

		GFNotExpression NotT = new GFNotExpression(T);
		GFNotExpression NotV = new GFNotExpression(V);
		GFNotExpression NotW = new GFNotExpression(W);
		
		GFAndExpression AndST = new GFAndExpression(S,NotT);
		GFOrExpression OrUV = new GFOrExpression(U,NotV);

		GFAndExpression AndSTUV = new GFAndExpression(AndST,OrUV);
		GFAndExpression STUVW = new GFAndExpression(AndSTUV,NotW);
		
		GFExistentialExpression be = new GFExistentialExpression(R, STUVW, new GFAtomicExpression("O", "x1"));
		
		System.out.println(be);
		DNFConverter converter = new DNFConverter();
		System.out.println(converter.convert(be));
	}

}
