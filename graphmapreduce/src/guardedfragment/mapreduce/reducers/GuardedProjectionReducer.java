package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.booleanexpressions.BEvaluationContext;
import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.VariableNotFoundException;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;
import guardedfragment.structure.conversion.GFtoBooleanConvertor;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.DeserializeException;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mapreduce.data.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Round 2 Reducer
 * 
 * Input: Si(a,b) : set of tuples Output: Si(a,b);R(a',b') (note the semicolon!)
 * 
 * Configuration: Guarding relation R, Guarded relations Si
 * 
 * This reducer checks for each data tuple Si(a,b) whether: - it appears in R -
 * it appears in Si
 * 
 * When this is the case, both existing tuples are output.
 * 
 * 
 * @author Jonny Daenen
 * @author Tony Tan
 * 
 */
public class GuardedProjectionReducer extends Reducer<Text, Text, Text, Text> {

	// GFExistentialExpression formula;
	Set<GFExistentialExpression> formulaSet;
	

	GFtoBooleanConvertor convertor;

	private static final Log LOG = LogFactory.getLog(GuardedProjectionReducer.class);

	public GuardedProjectionReducer() {
		super();

	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();

		convertor = new GFtoBooleanConvertor();
		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}

		} catch (DeserializeException e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String s = key.toString();
		Tuple keyTuple = new Tuple(s);

		// convert value set to tuple set
		Set<Tuple> tuples = new HashSet<Tuple>();

		for (Text t : values) {
			// skip empty values (only used for propagation)
			if (t.toString().length() == 0)
				continue;

			Tuple ntuple = new Tuple(t.toString());
			tuples.add(ntuple);
		}

		for (GFExistentialExpression formula : formulaSet) {

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
						context.write(null, new Text(outputTuple));
					}
				} catch (VariableNotFoundException | NonMatchingTupleException e) {
					// should not happen
					LOG.error("Unexpected exception: " + e.getMessage());
					e.printStackTrace();
				}

			}

		}

	}

}
