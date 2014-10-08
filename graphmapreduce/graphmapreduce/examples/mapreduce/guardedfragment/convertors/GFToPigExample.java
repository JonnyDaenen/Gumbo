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
//		queries.add("#O(x1,x2)&R(x1,x2)&!S1(x1)&!S2(x2)S3(x1)");
		queries.add("#Out(x2)&R(x2,x3,x4,x5)&!S2(x2)&!S3(x3)&!S4(x4)!S5(x5)");
//		queries.add("#P(x1,x2)&R(x1,x2)O(x2,x2)");
//		queries.add("#Q(x1,x2)&R(x1,x2)O(x1,x1)");

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();
		
		Collection<GFExpression> gfes1 = parser.deserialize(queries);
		GFExistentialExpression ge = (GFExistentialExpression) gfes1.iterator().next();
		System.out.println(ge);
		
		GFToPig convertor = new GFToPig();
		String result = convertor.convert(ge);
		System.out.println(result);
	}
}
