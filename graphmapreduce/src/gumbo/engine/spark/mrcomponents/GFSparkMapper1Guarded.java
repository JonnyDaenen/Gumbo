/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkMapper1Guarded extends GFSparkComponent implements PairFlatMapFunction<String, String, String> {


	private static final long serialVersionUID = 1L;

	public GFSparkMapper1Guarded(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);
	}

	

	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(String value) throws Exception {
		
		Set<Tuple2<String,String>> output = new HashSet<>();
		
		// emit proof of existance
		output.add(new Tuple2<>(value,settings.getProperty(AbstractExecutorSettings.PROOF_SYMBOL)));
		
		return output;
	}

}
