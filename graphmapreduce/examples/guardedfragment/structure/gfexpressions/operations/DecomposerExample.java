/**
 * Created: 28 Apr 2014
 */
package guardedfragment.structure.gfexpressions.operations;

import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import java.util.Set;

/**
 * @author Jonny Daenen
 *
 */
public class DecomposerExample {

	public static void main(String[] args) throws Exception {
		
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		GFDecomposer decomposer = new GFDecomposer();
		
		GFExpression exp = serializer.deserialize("#O(x,y)&G(x,y)!|#O1(y)&G(x,y)S(x)R(x,y)");
		
		Set<GFExistentialExpression> basics = decomposer.decompose(exp);
		
		System.out.println(exp);
		System.out.println("Basic expressions:");
		System.out.println(basics);
		
		
	}
}
