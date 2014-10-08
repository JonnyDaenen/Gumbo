/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.io.IOException;

import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Jonny Daenen
 *
 */
public abstract class GFReducer extends ExpressionSetOperation {
	

	


	/**
	 * 
	 * @param key the group key
	 * @param values the set of values associated with the group key
	 * @param expressionSet the set of formulas that are being calculated
	 * @return A set of reduced values, where each value is assosiated with an output file.
	 * 
	 * OPTIMIZE give some kind of context to write to
	 */
	public abstract Iterable<Pair<Text, String>> reduce(Text key, Iterable<? extends Object> values) throws GFOperationInitException;


	/**
	 * @param key
	 * @param values
	 * @param context
	 * @return
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void reduce(Text key, Iterable<Text> values,
			MultipleOutputs<Text, Text> mos) throws IOException, InterruptedException {
		
		try {
			Iterable<Pair<Text, String>> result = reduce(key, values);

			for (Pair<Text, String> pair : result) {
				Text value = pair.fst;
				String filename = pair.snd;

				// LOG.debug("writing " + value + " to " + filename);
				mos.write((Text) null, value, filename);

			}
		} catch (GFOperationInitException e) {
			throw new InterruptedException(e.getMessage());
		}
		
	}

}
