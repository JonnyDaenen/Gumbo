package guardedfragment.mapreduce.reducers;

import guardedfragment.booleanstructure.BEvaluationContext;
import guardedfragment.booleanstructure.BExpression;
import guardedfragment.booleanstructure.VariableNotFoundException;
import guardedfragment.structure.DeserializeException;
import guardedfragment.structure.GFAtomicExpression;
import guardedfragment.structure.GFBMapping;
import guardedfragment.structure.GFConversionException;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFExpression;
import guardedfragment.structure.GFSerializer;
import guardedfragment.structure.GuardedProjection;
import guardedfragment.structure.MyGFParser;
import guardedfragment.structure.NonMatchingTupleException;

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
	GFAtomicExpression output;
	MyGFParser parser;
	//String[] freevars;

	
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
			parser = new MyGFParser(conf.get("formula"));
			GFExistentialExpression f = (GFExistentialExpression) parser.deserialize();
			
			//this.guard = serializer.deserializeGuard(conf.get("guard"));
			//this.child = serializer.deserializeGFBoolean(conf.get("booleanformula"));
			//this.output = serializer.deserializeGFAtom(conf.get("outputname"));
			this.guard = f.getGuard();
			this.child = f.getChild();
			this.output = f.getOutputSchema();

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
		
		GuardedProjection p = new GuardedProjection(guard,output);
		
		String s = key.toString();
		Tuple tkey = new Tuple(s);
		if (guard.matches(tkey)) {

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
					context.write(null, new Text(p.project(tkey).generateString()));
					
					// FIXME we should add some projection
				}
			} catch (VariableNotFoundException | NonMatchingTupleException e) {
				// should not happen
				e.printStackTrace();
			}

		}

	}



}
