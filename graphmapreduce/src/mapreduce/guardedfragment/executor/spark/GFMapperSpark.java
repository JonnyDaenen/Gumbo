/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.executor.spark;

import java.util.Collection;
import java.util.HashSet;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

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

		Iterable<Pair<String, String>> mappedResult = mapper.map(value.toString());

		for (Pair<String, String> pair : mappedResult) {
			String k = pair.fst;
			String val = pair.snd;
			result.add(new Tuple2<String, String>(k, val));
		}

		return result;
	}

}
