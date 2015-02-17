/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.abstractMR;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFOperationInitException;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Jonny Daenen
 *
 */
public abstract class GFReducer extends ExpressionSetOperations {
	

	
	public GFReducer(Collection<GFExistentialExpression> expressionSet, RelationFileMapping fileMapping)
			throws GFOperationInitException {
		super(expressionSet, fileMapping);
		// TODO Auto-generated constructor stub
	}


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
