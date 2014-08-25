/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.convertors;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

/**
 * @author Jonny Daenen
 *
 */
public class GFToPigExample {

	public static void main(String[] args) throws DeserializeException, GFConversionException {
		Set<String> queries = new HashSet<String>();
		queries.add("#O(x1,x2)&R(x1,x2)&!S1(x1)!S2(x2)");
//		queries.add("#P(x1,x2)&R(x1,x2)O(x2,x2)");
//		queries.add("#Q(x1,x2)&R(x1,x2)O(x1,x1)");

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();
		
		Collection<GFExpression> gfes1 = parser.deserialize(queries);
		
		GFToPig convertor = new GFToPig();
		String result = convertor.convert((GFExistentialExpression) gfes1.iterator().next());
		System.out.println(result);
	}
}
