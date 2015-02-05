/**
 * Created: 22 Aug 2014
 */
package gumbo.compiler.resolver.reducers;

import gumbo.compiler.structures.data.RelationSchema;
import gumbo.compiler.structures.data.Tuple;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.compiler.structures.operations.GFReducer;
import gumbo.guardedfragment.booleanexpressions.BEvaluationContext;
import gumbo.guardedfragment.booleanexpressions.BExpression;
import gumbo.guardedfragment.booleanexpressions.VariableNotFoundException;
import gumbo.guardedfragment.conversion.GFBooleanMapping;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.Pair;
import gumbo.guardedfragment.gfexpressions.operations.GFAtomProjection;
import gumbo.guardedfragment.gfexpressions.operations.NonMatchingTupleException;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * @author Jonny Daenen
 * 
 */
public class GFReducer2Generic extends GFReducer implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(GFReducer2Generic.class);

	boolean receiveIDs = true;

	/**
	 * @throws GFOperationInitException
	 * @see gumbo.compiler.structures.operations.GFReducer#reduce(java.lang.String,
	 *      java.lang.Iterable, java.util.Collection)
	 */
	@Override
	public Iterable<Pair<Text, String>> reduce(Text key, Iterable<? extends Object> values)
			throws GFOperationInitException {

		try {
			HashSet<Pair<Text, String>> result = new HashSet<Pair<Text, String>>();

			Tuple keyTuple = new Tuple(key);

			/* create boolean value mapping */
			GFBooleanMapping mapGFtoB = getBooleanMapping();
			BEvaluationContext booleanContext = new BEvaluationContext();

			for (Object v : values) {
				Text t = (Text) v;

				// skip empty values (only used for propagation)
				if (t.getLength() == 0)
					continue;

				// atoms that are present are "true"
				// id mode vs string mode
				if (receiveIDs) {
					GFAtomicExpression atom = getAtom(Integer.parseInt(t.toString())); // OPTIMIZE use LongWritable
					booleanContext.setValue(mapGFtoB.getVariable(atom), true);
				} else {
					Tuple tuple = new Tuple(t);
					GFAtomicExpression dummy = new GFAtomicExpression(tuple.getName(), tuple.getAllData());
					booleanContext.setValue(mapGFtoB.getVariable(dummy), true);
				}
			}

			/* evaluate all formulas */
			for (GFExistentialExpression formula : expressionSet) {

				// only if applicable
				GFAtomicExpression guard = formula.getGuard();
				if (guard.matches(keyTuple)) {

					// convert to boolean expression
					BExpression booleanChildExpression = getBooleanChildExpression(formula);

					// determine output
					GFAtomicExpression output = formula.getOutputRelation();
					GFAtomProjection p = getOutputProjection(formula);
					String outfile = generateFileName(p.getOutputSchema());

					// evaluate
					boolean eval = booleanChildExpression.evaluate(booleanContext);
					if (eval) {

						// project the tuple and output it
						String outputTuple = p.project(keyTuple).generateString();
						result.add(new Pair<Text, String>(new Text(outputTuple), outfile));
					}
				}

			}

			return result;
		} catch (VariableNotFoundException | NonMatchingTupleException e) {
			// should not happen
			LOG.error("Unexpected exception: " + e.getMessage());
			e.printStackTrace();
			throw new GFOperationInitException(e);
		}

	}

	public static String generateFileName(RelationSchema relationSchema) {
		String rel = relationSchema.getShortDescription();
		String name = generateFolder(relationSchema) + "/" + rel;
		// LOG.info("file:" + name);
		return name;
	}

	public static String generateFolder(RelationSchema relationSchema) {
		return relationSchema.getShortDescription();
	}

}
