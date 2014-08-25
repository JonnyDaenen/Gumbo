/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.executor.spark;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFReducer;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 * 
 */
public class GFReducerSpark implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {

	GFReducer reducer;

	Collection<GFExistentialExpression> expressionSet;

	/**
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 */
	public GFReducerSpark(Class<? extends GFReducer> reducerclass, Collection<GFExistentialExpression> expressionSet) throws InstantiationException, IllegalAccessException {
		this.reducer = reducerclass.newInstance();
		this.expressionSet = expressionSet;
	}

	/**
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<String> call(Tuple2<String, Iterable<String>> keyvalues) throws Exception {

		String key = keyvalues._1;
		Iterable<String> values = keyvalues._2;

		Set<Pair<String, String>> redresult = reducer.reduce(key, values, expressionSet);

		HashSet<String> result = new HashSet<String>();

		// TODO just make an iterable, this goes wrong with big data
		// OPTIMIZE split into different RDDs?
		for (Pair<String, String> pair : redresult) {
			result.add(pair.fst); // only get the data, ignore the filename
		}

		return result;

	}

}
