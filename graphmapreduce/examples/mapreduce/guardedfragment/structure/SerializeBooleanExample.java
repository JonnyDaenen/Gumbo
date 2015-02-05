/**
 * Created: 31 Mar 2014
 */
package mapreduce.guardedfragment.structure;

import gumbo.guardedfragment.gfexpressions.GFAndExpression;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.GFNotExpression;
import gumbo.guardedfragment.gfexpressions.GFOrExpression;
import gumbo.guardedfragment.gfexpressions.io.GFInfixSerializer;


/**
 * @author Jonny Daenen
 *
 */
public class SerializeBooleanExample {
	
	public static void main(String[] args) throws Exception {
		
		GFAtomicExpression gfe1 = new GFAtomicExpression("R", "x", "y", "x");
		GFAtomicExpression gfe2 = new GFAtomicExpression("S", "x", "y", "x");
		GFAtomicExpression gfe3 = new GFAtomicExpression("T", "x", "y", "x");
		
		GFAndExpression gfe4 = new GFAndExpression(gfe1, gfe2);
		GFNotExpression gfe5 = new GFNotExpression(gfe4);
		GFNotExpression gfe6 = new GFNotExpression(gfe3);
		GFOrExpression gfe7 = new GFOrExpression(gfe5, gfe6);
		
		GFInfixSerializer serializer = new GFInfixSerializer();
		String ser = serializer.serializeGFBoolean(gfe7);
		System.out.println(ser);
		
		GFExpression deser = serializer.deserializeGFBoolean(ser);
		System.out.println(deser);
		
		
		
	}

}
