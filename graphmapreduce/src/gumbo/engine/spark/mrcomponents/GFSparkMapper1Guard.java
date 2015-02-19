/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * TODO add eso to serialization?
 * @author Jonny Daenen
 *
 */
public class GFSparkMapper1Guard extends GFSparkComponent implements PairFlatMapFunction<String, String, String> {


	private static final long serialVersionUID = 1L;


	public GFSparkMapper1Guard(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}


	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(String arg0) throws Exception {
		
		// for each expression in which the guard matches
		// for each guarded atom
		// TODO emit value request
		
		return null;
	}

}
