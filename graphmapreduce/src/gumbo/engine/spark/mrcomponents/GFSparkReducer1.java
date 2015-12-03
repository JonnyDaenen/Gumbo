/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import java.util.HashSet;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkReducer1 extends GFSparkComponent implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String> {

	private static final long serialVersionUID = 1L;

	StringBuilder sb;
	
	public GFSparkReducer1() {
		// TODO Auto-generated constructor stub
		sb = new StringBuilder(100);
	}
	
	public GFSparkReducer1(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		super(eso, settings);

		sb = new StringBuilder(100);
	}



	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> kvset) throws Exception {
		
		
		// extract  valueset
		Iterable<String> values = kvset._2;
		
		
		HashSet<Tuple2<String, String>> output = new HashSet<>();
		boolean keyFound = false;
		
		// for each value
		for (String value : values) {
			// split on ;
			Pair<String, Integer> split = split(value);
			
			// if the ';' is found
			if (split.snd != -1) {
				output.add(new Tuple2<>(split.fst,""+split.snd)); // OPTIMIZE make this integer
				
			}
			// otherwise this is a POE message
			else {
				keyFound = true;
			}
			
		}
		
		// if the key is not found, we cannot send anything
		if (!keyFound) {
			output.clear();
		}

			
		return output;
	}

	
	/**
	 * Splits Stirng into 2 parts. String is supposed to be separated with ';'.
	 * When no ';' is present, the numeric value is -1. 
	 * @param t
	 */
	protected Pair<String, Integer> split(String t) {
		int length = t.length();
		String output = null;
		int num = -1;
		sb.setLength(0);
		boolean numberPart = false;

		byte[] b = t.getBytes();
		for (int i = 0; i < length; i++) { // FUTURE for unicode this doesn't
			// work I guess..
			char c = (char)b[i];
			// if we find the semicolon
			if (c == ';') {
				numberPart = true;
				num = 0;
				// assemble number
			} else if (numberPart && ( '0' <= c && c <= '9')){
				num *= 10;
				num +=  c - '0';

			} else {
				sb.append((char) b[i]);
			}
		}

		output = sb.toString();

		return new Pair<>(output, num);

	}

}
