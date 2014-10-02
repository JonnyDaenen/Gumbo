/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

/**
 * interface for a map function.
 * 
 * @author Jonny Daenen
 * 
 */
public abstract class GFMapper extends ExpressionSetOperation {

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

}
