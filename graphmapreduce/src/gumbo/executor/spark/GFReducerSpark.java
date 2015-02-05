/**
 * Created: 25 Aug 2014
 */
package gumbo.executor.spark;

import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.compiler.structures.operations.GFReducer;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.Pair;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
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
	public GFReducerSpark(Class<? extends GFReducer> reducerclass, Collection<GFExistentialExpression> expressionSet)
			throws InstantiationException, IllegalAccessException {
		try {
			this.reducer = reducerclass.newInstance();

			this.reducer.setExpressionSet(expressionSet);
			this.expressionSet = expressionSet;
		} catch (GFOperationInitException e) {
			throw new InstantiationException(e.getMessage());
		}
	}

	/**
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<String> call(Tuple2<String, Iterable<String>> keyvalues) throws Exception {

		String key = keyvalues._1;
		Iterable<String> values = keyvalues._2;

		Iterable<Pair<Text, String>> redresult = reducer.reduce(new Text(key), values);

		HashSet<String> result = new HashSet<String>();

		// TODO just make an iterable, this goes wrong with big data
		// OPTIMIZE split into different RDDs?
		for (Pair<Text, String> pair : redresult) {
			result.add(pair.fst.toString()); // only get the data, ignore the filename
		}

		return result;

	}

}
