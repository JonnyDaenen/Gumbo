/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.io.IOException;
import java.util.Set;

import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * interface for a map function.
 * 
 * @author Jonny Daenen
 * 
 */
public abstract class GFMapper extends ExpressionSetOperations {

	// TODO remove this
	/**
	 * Add a KV-pair to a resultset.
	 * 
	 * @param text
	 *            the key
	 * @param text2
	 *            the value
	 * @param result
	 *            the resultset to add the pair to
	 */
	protected void addOutput(Text text, Text text2, Set<Pair<Text, Text>> result) {

		Pair<Text, Text> p = new Pair<Text, Text>(text, text2);
		result.add(p);

	}

	// OPTIMIZE maybe use a pipe for output as does Hadoop
	public abstract Iterable<Pair<Text, Text>> map(Text value) throws GFOperationInitException;

	/**
	 * @param value
	 * @param value 
	 * @param context
	 * @return
	 * @throws GFOperationInitException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void map(Text key, Text value, Context context) throws GFOperationInitException, IOException, InterruptedException {
		Iterable<Pair<Text, Text>> result = map(value);
		
		for (Pair<Text, Text> pair : result) {
			Text k = pair.fst;
			Text val = pair.snd;
			context.write(k,val);
		}
		
	}

}
