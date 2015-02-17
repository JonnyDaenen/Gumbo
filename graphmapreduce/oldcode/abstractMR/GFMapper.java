/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.hadoop.abstractMR;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * interface for a map function.
 * 
 * @author Jonny Daenen
 * 
 */
public abstract class GFMapper extends ExpressionSetOperations {


	public GFMapper(Collection<GFExistentialExpression> expressionSet, RelationFileMapping fileMapping)
			throws GFOperationInitException {
		super(expressionSet, fileMapping);
		// TODO Auto-generated constructor stub
	}

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
