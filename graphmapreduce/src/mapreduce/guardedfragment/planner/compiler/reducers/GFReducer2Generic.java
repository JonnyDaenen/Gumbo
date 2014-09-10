/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.reducers;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFReducer;
import mapreduce.guardedfragment.structure.booleanexpressions.BEvaluationContext;
import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.booleanexpressions.VariableNotFoundException;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConversionException;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConvertor;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 *
 */
public class GFReducer2Generic extends GFReducer implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(GFReducer2Generic.class);

	private static GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
	
	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFReducer#reduce(java.lang.String, java.lang.Iterable, java.util.Collection)
	 */
	@Override
	public Set<Pair<String, String>> reduce(String key, Iterable<? extends Object> values,
			Collection<GFExistentialExpression> expressionSet) {
		
		HashSet<Pair<String, String>> result = new HashSet<Pair<String, String>>();

		
		String s = key.toString();
		Tuple keyTuple = new Tuple(s);
		


		// convert value set to tuple set
		Set<Tuple> tuples = new HashSet<Tuple>();

		for (Object t : values) {
			// skip empty values (only used for propagation)
			if (t.toString().length() == 0)
				continue;

			Tuple ntuple = new Tuple(t.toString());
			tuples.add(ntuple);
		}

		for (GFExistentialExpression formula : expressionSet) {

			// only basic formula's
			if (!formula.isBasicGF()) {
				LOG.error("Non-basic GF expression found, skipping: " + formula);
				continue;
			}

			// 3 crucial parts of the expression
			GFAtomicExpression output = formula.getOutputRelation();
			GFAtomicExpression guard = formula.getGuard();
			GFExpression child = formula.getChild();
			Set<GFAtomicExpression> allAtoms = child.getAtomic();

			// calculate projection to output relation
			// OPTIMIZE this can be done in advance
			GFAtomProjection p = new GFAtomProjection(guard, output);
			String outfile = generateFileName(p.getOutputSchema());

			// convert to boolean formula, while constructing the mapping
			// automatically
			BExpression booleanChildExpression = null;
			// mapping will be created by convertor
			GFBooleanMapping mapGFtoB = null;
			try {
				booleanChildExpression = convertor.convert(child);
				mapGFtoB = convertor.getMapping();

			} catch (GFtoBooleanConversionException e1) {
				LOG.error("Something went wrong when converting GF to boolean, skipping: " + e1.getMessage());
				continue;
			}

			// if this tuple applies to the current formula
			if (guard.matches(keyTuple)) {

				// Create a boolean context, and set all atoms to false
				// initially
				BEvaluationContext booleanContext = new BEvaluationContext();
				for (GFAtomicExpression atom : allAtoms) {
					booleanContext.setValue(mapGFtoB.getVariable(atom), false);
				}

				// atoms that appear as values are set to true
				for (Tuple tuple : tuples) {
					GFAtomicExpression dummy = new GFAtomicExpression(tuple.getName(), tuple.getAllData());
					booleanContext.setValue(mapGFtoB.getVariable(dummy), true);
				}

				// evaluate boolean formula using the created context
				try {
					if (booleanChildExpression.evaluate(booleanContext)) {
						// project the tuple and output it
						String outputTuple = p.project(keyTuple).generateString();
						// context.write(null, new Text(outputTuple));
//						mos.write((Text) null, new Text(outputTuple), outfile);
						result.add(new Pair<String, String>(outputTuple, outfile));
					}
				} catch (VariableNotFoundException | NonMatchingTupleException e) {
					// should not happen
					LOG.error("Unexpected exception: " + e.getMessage());
					e.printStackTrace();
				}

			}

		}
		return result;
		
	}
	

	public static String generateFileName(RelationSchema relationSchema) {
		String rel = relationSchema.getShortDescription();
		String name = generateFolder(relationSchema) + "/" + rel;
//		LOG.info("file:" + name);
		return name;
	}
	
	public static String generateFolder(RelationSchema relationSchema) {
		return relationSchema.getShortDescription();
	}

	
	
}
