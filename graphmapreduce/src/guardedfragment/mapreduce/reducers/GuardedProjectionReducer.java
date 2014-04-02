package guardedfragment.mapreduce.reducers;

import guardedfragment.booleanstructure.BEvaluationContext;
import guardedfragment.booleanstructure.BExpression;
import guardedfragment.booleanstructure.VariableNotFoundException;
import guardedfragment.structure.DeserializeException;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFBMapping;
import guardedfragment.structure.GFConversionException;
import guardedfragment.structure.GFExpression;
import guardedfragment.structure.GFSerializer;

import java.io.IOException;
import java.util.Set;

import mapreduce.data.Tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Phase: Basic Guarded - Phase 2 Reducer
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

	GFAtomicExpression guard;
	GFExpression child;
	GFBMapping mapGFtoB;
	BExpression Bchild;
	
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

		GFSerializer serializer = new GFSerializer();
		Configuration conf = context.getConfiguration();

		try {
			// get parameters
			this.guard = serializer.deserializeGuard(conf.get("guard"));
			this.child = serializer.deserializeGFBoolean(conf.get("booleanformula"));

			// create mapping
			mapGFtoB = new GFBMapping();
			Set<GFAtomicExpression> allAtom = child.getAtomic();
			for (GFAtomicExpression atom : allAtom) {
				mapGFtoB.insertElement(atom);
			}

			Bchild = child.convertToBExpression(mapGFtoB);
			
		} catch (GFConversionException e) {
			// should not happen!
			e.printStackTrace();
			throw new InterruptedException("Error during convertion to boolean formula");
			
		} catch (DeserializeException e) {
			e.printStackTrace();
			throw new InterruptedException("Error during parameter recovery");
		}
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		LOG.error("boolean: " + child);
		LOG.error("boolean: " + Bchild);
		LOG.error("key: " + key.toString());
		
		String s = key.toString();
		if (guard.matches(new Tuple(s))) {

			// get all guarded relations from the formula
			BEvaluationContext BchildEval = new BEvaluationContext();
			Set<GFAtomicExpression> allAtom = child.getAtomic();
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
					context.write(null, key);
					
					// FIXME we should add some projection
				}
			} catch (VariableNotFoundException e) {
				// should not happen
				e.printStackTrace();
			}

		}

	}

	/*
	 * protected void oldreduce(Text key, Iterable<Text> values, Context
	 * context) throws IOException, InterruptedException {
	 * 
	 * Set<RelationSchema> foundRelations = new HashSet<RelationSchema>();
	 * Set<Tuple> outputTuples = new HashSet<Tuple>();
	 * 
	 * // boolean allRelationsFound = false;
	 * 
	 * // Record presence of all required relations for (Text value : values) {
	 * Tuple t = new Tuple(value.toString());
	 * 
	 * if (t.satisfiesSchema(outputrelation)) outputTuples.add(t);
	 * 
	 * RelationSchema s = t.extractSchema();
	 * 
	 * if(relations.contains(s)) foundRelations.add(s);
	 * 
	 * }
	 * 
	 * // if all relations are found if(foundRelations.size() ==
	 * relations.size()) for (Tuple t : outputTuples) { String value =
	 * t.generateString(); context.write(key, new Text(value)); }
	 * 
	 * }
	 */

}
