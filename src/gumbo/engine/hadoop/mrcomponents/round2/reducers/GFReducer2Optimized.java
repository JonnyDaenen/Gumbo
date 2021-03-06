/**
 * Created: 16 May 2015
 */
package gumbo.engine.hadoop.mrcomponents.round2.reducers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import gumbo.engine.general.algorithms.Red2Algorithm;
import gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactory;
import gumbo.engine.hadoop.mrcomponents.tools.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

/**
 * Uses atom data generated by the corresponding mapper.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFReducer2Optimized extends Reducer<Text, Text, Text, Text> {



	private static final Log LOG = LogFactory.getLog(GFReducer2Optimized.class);

	private Red2Algorithm algo;


	private ExpressionSetOperations eso;



	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);

		String s = String.format("Reducer"+this.getClass().getSimpleName()+"-%05d-%d",
				context.getTaskAttemptID().getTaskID().getId(),
				context.getTaskAttemptID().getId());
		LOG.info(s);



		// load parameters
		try {
			Configuration conf = context.getConfiguration();

			ParameterPasser pp = new ParameterPasser(conf);
			eso = pp.loadESO();
			HadoopExecutorSettings settings = pp.loadSettings();

			Red2MessageFactory msgFactory = new Red2MessageFactory(context, settings, eso);
			algo = new Red2Algorithm(eso, settings, msgFactory);
			
			
			

		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		try{
			algo.cleanup();
			
		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		try {

			algo.initialize(key.toString());

			// WARNING Text object will be reused by Hadoop!
			for (Text t : values) {

				// feed it to algo
				if (!algo.processTuple(t.toString()))
					break;

			}

			// indicate end of tuples
			// and finish calculation
			algo.finish();

		} catch(Exception e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
		//		try {
		//
		//			GFBooleanMapping mapGFtoB = eso.getBooleanMapping();
		//			BEvaluationContext booleanContext = new BEvaluationContext();
		//
		//			boolean lookingForGuard = guardRefOn;
		//
		//			Tuple guardTuple = null;
		//			if (!lookingForGuard)
		//				guardTuple = new Tuple(key.getBytes(),key.getLength());
		//
		//			for (Text v : values) {
		//
		//				String value = v.toString();
		//
		//				// record guard tuple if found
		//				if (msgFactory.isTuple(value)) {
		//					if (lookingForGuard) {
		//						guardTuple = msgFactory.getTuple(value);
		//						lookingForGuard = false;
		//					} else
		//						continue;
		//				} 
		//				// otherwise we keep track of true atoms
		//				else {
		//					// extract atom reference and set it to true
		//					String atomRef = value;
		//					BVariable atom;
		//
		//					if (atomIdOn) {
		//						int id = Integer.parseInt(atomRef);
		//						GFAtomicExpression atomExp = eso.getAtom(id); 
		//						atom = mapGFtoB.getVariable(atomExp);
		//					} else {
		//						Tuple atomTuple = new Tuple(atomRef);
		//						GFAtomicExpression dummy = new GFAtomicExpression(atomTuple.getName(), atomTuple.getAllData());
		//						atom = mapGFtoB.getVariable(dummy);
		//					}
		//					booleanContext.setValue(atom, true);
		//
		//					
		//				}
		//
		//
		//			}
		//
		//			
		//
		//			// if guard tuple is not present yet, throw exception
		//			if (lookingForGuard) {
		//				EXCEPT.increment(1);
		//				throw new GuardTupleNotFoundException("There was no guard tuple found for key "+ key.toString());
		//			} else {
		//				TUPLES.increment(1);
		//			}
		//
		//			/* evaluate all formulas */
		//			for (GFExistentialExpression formula : eso.getExpressionSet()) {
		//
		//				// only if applicable
		//				GFAtomicExpression guard = formula.getGuard();
		//				if (guard.matches(guardTuple)) {
		//
		//					// get associated boolean expression
		//					BExpression booleanChildExpression = eso.getBooleanChildExpression(formula);
		//
		//					// evaluate
		//					boolean eval = booleanChildExpression.evaluate(booleanContext);
		//					
		//					if (eval) {
		//
		//						// calculate output tuple
		//						GFAtomProjection p = eso.getOutputProjection(formula);
		//
		//						// send it
		//						TRUE.increment(1);
		//						msgFactory.loadValue(p.project(guardTuple));
		//						msgFactory.sendOutput();
		//
		//
		//					} else {
		//						FALSE.increment(1);
		//					}
		//
		//				}
		//			}
		//
		//
		//
		//
		//		} catch (VariableNotFoundException | NonMatchingTupleException | GFOperationInitException | GuardTupleNotFoundException e) {
		//			// should not happen
		//			LOG.error(e.getMessage());
		//			e.printStackTrace();
		//			throw new InterruptedException(e.getMessage());
		//		} 
	}


	


}
