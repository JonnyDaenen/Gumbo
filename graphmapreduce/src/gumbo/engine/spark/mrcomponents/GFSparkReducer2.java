/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkReducer2 extends GFSparkComponent implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {


	private static final long serialVersionUID = 1L;
	

	public GFSparkReducer2(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}


	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<String> call(Tuple2<String, Iterable<String>> arg0) throws Exception {
		
		// TODO implement
		// for each value
		// buffer true atom ids
		// store tuple if found
		
		// if tuple is there
		// evaluate formula
		// output iff true
		
		return null;
	}

}
