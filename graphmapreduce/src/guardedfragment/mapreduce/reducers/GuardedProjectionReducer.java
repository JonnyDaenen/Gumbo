package guardedfragment.mapreduce.reducers;

import guardedfragment.structure.booleanexpressions.BEvaluationContext;
import guardedfragment.structure.booleanexpressions.BExpression;
import guardedfragment.structure.booleanexpressions.VariableNotFoundException;
import guardedfragment.structure.conversion.GFBooleanMapping;
import guardedfragment.structure.conversion.GFtoBooleanConversionException;
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

	
	//GFExistentialExpression formula;
	Set<GFExistentialExpression> formulaSet;

	
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
		
		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);
			
			
			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if(exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}
			
		} catch (DeserializeException e) {
			throw new InterruptedException("Mapper initialisation error: "+ e.getMessage()); 
		}
						
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (GFExistentialExpression formula: formulaSet) {
			
			GFExpression child = formula.getChild();
			GFBooleanMapping mapGFtoB;
			GFAtomicExpression output = formula.getOutputSchema();
			GFAtomicExpression guard = formula.getGuard();
			
			// create mapping
			mapGFtoB = new GFBooleanMapping();
			Set<GFAtomicExpression> allAtom = child.getAtomic();
			for (GFAtomicExpression atom : allAtom) {
				mapGFtoB.insertElement(atom);
			}


			BExpression Bchild = null;
			try {
				Bchild = child.convertToBExpression(mapGFtoB);
			} catch (GFtoBooleanConversionException e1) {
				e1.printStackTrace();
			}
			
			GFAtomProjection p = new GFAtomProjection(guard,output);
			
			String s = key.toString();
			Tuple tkey = new Tuple(s);
			if (guard.matches(tkey)) {

				BEvaluationContext BchildEval = new BEvaluationContext();
				LOG.error("atomics: " + allAtom);

				// set them all to false
				for (GFAtomicExpression atom : allAtom) {
					BchildEval.setValue(mapGFtoB.getVariable(atom), false);
				}


				
				Tuple t;
				GFAtomicExpression dummy;
				String sd;
				// set certain values to true
				for (Text value : values) {
					sd = value.toString();
					if (sd.length() != 0) {
						LOG.error("value: " + value);
						t = new Tuple(value.toString());
						dummy = new GFAtomicExpression(t.getName(), t.getAllData());
						
						BchildEval.setValue(mapGFtoB.getVariable(dummy), true);

						LOG.error("EVALUATING THE CHILD: " + BchildEval);
						
					}
				}

				LOG.error("=====================");
				
				try {
					if (Bchild.evaluate(BchildEval)) {
						context.write(null, new Text(p.project(tkey).generateString()));
					}
				} catch (VariableNotFoundException | NonMatchingTupleException e) {
					// should not happen
					e.printStackTrace();
				}

			}

						
		}
		
		
	}



}
