/**
 * Created: 25 Aug 2014
 */
package gumbo.executor.spark;

import gumbo.compiler.structures.operations.GFMapper;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.Pair;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 * 
 */
public class GFMapperSpark implements PairFlatMapFunction<String, String, String> {

	Collection<GFExistentialExpression> expressionSet;
	GFMapper mapper;

	/**
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 
	 */
	public GFMapperSpark(Class<? extends GFMapper> mapperclass, Collection<GFExistentialExpression> expressionSet)
			throws InstantiationException, IllegalAccessException {

		try {
			this.mapper = mapperclass.newInstance();
			this.mapper.setExpressionSet(expressionSet);
			this.expressionSet = expressionSet;
		} catch (GFOperationInitException e) {
			throw new InstantiationException(e.getMessage());
		}
	}

	/**
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception {

		HashSet<Tuple2<String, String>> result = new HashSet<Tuple2<String, String>>();

		Iterable<Pair<Text, Text>> mappedResult = mapper.map(new Text(value));

		for (Pair<Text, Text> pair : mappedResult) {
			String k = pair.fst.toString();
			String val = pair.snd.toString();
			result.add(new Tuple2<String, String>(k, val));
		}

		return result;
	}

}
