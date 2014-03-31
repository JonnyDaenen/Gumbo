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
public class SerializeExample {
	
	public static void main(String[] args) throws Exception {
		
		GFAtomicExpression gfe1 = new GFAtomicExpression("R", "x", "y", "x");
		
		GFSerializer ser = new GFSerializer();
		String s1 = ser.serializeGuard(gfe1);
		GFAtomicExpression gfe2 = ser.deserializeGuard(s1);
		
		System.out.println(gfe1);
		System.out.println(gfe2);
		System.out.println(gfe1.equals(gfe2));
		
		
		HashSet<GFAtomicExpression> set1 = new HashSet<GFAtomicExpression>();
		set1.add(gfe1);
		set1.add(new GFAtomicExpression("S", "y"));
		String s2 = ser.serializeGuarded(set1);
		Set<GFAtomicExpression> set2 = ser.deserializeGuarded(s2);
		
		System.out.println(set1);
		System.out.println(set2);
		
		
		
	}

}
