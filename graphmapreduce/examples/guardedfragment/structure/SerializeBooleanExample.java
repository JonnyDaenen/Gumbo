/**
 * Created: 31 Mar 2014
 */
package guardedfragment.structure;

import java.util.HashSet;
import java.util.Set;

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
		GFOrExpression gfe6 = new GFOrExpression(gfe5, gfe3);
		
		GFSerializer serializer = new GFSerializer();
		String ser = serializer.serializeGFBoolean(gfe6);
		System.out.println(ser);
		
		GFExpression deser = serializer.deserializeGFBoolean(ser);
		System.out.println(deser);
		
		
		
	}

}
