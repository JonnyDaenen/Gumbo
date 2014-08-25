/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.executor.spark;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
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
	public GFMapperSpark(Class<? extends GFMapper> mapperclass, Collection<GFExistentialExpression> expressionSet) throws InstantiationException, IllegalAccessException {
		this.mapper = mapperclass.newInstance();
		this.expressionSet = expressionSet;
	}
	
	/**
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception {
		
		HashSet<Tuple2<String,String>> result = new HashSet<Tuple2<String,String>>();
		
		Set<Pair<String, String>> mappedResult = mapper.map(value.toString(), expressionSet);
		
		for (Pair<String, String> pair : mappedResult) {
			String k = pair.fst;
			String val = pair.snd;
			result.add(new Tuple2<String,String>(k,val));
		}
		
		return result;
	}

}
