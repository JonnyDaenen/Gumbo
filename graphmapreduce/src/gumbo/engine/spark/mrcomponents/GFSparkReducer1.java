/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkReducer1 extends GFSparkComponent implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String> {

	private static final long serialVersionUID = 1L;

	public GFSparkReducer1(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}



	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> arg0) throws Exception {
		
		// TODO implement
		// for each value
		// check for proof of existance
		// otherwise buffer action
		
		// only emit if proof of existance was found
		return null;
	}

}
