/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.ExecutorSettings;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.booleanexpressions.BEvaluationContext;
import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.booleanexpressions.VariableNotFoundException;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Uses atom data generated by the corresponding mapper.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFReducer2Text extends Reducer<Text, Text, Text, Text> {


	public class GuardTupleNotFoundException extends Exception {
		private static final long serialVersionUID = 1L;

		public GuardTupleNotFoundException(String msg) {
			super(msg);
		}
	}

	Text out1 = new Text();
	Text out2 = new Text();
	private ExpressionSetOperations eso;
	private MultipleOutputs<Text, Text> mos;

	private HashMap<RelationSchema, String> filenames;

	private static final Log LOG = LogFactory.getLog(GFReducer2Text.class);

	boolean receiveIDs = true;
	private ExecutorSettings settings;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();

		mos = new MultipleOutputs<>(context);
		filenames = new HashMap<RelationSchema, String>();

		String s = String.format("Reducer"+this.getClass().getSimpleName()+"-%05d-%d",
				context.getTaskAttemptID().getTaskID().getId(),
				context.getTaskAttemptID().getId());
		LOG.info(s);

		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			HashSet<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}

			eso = new ExpressionSetOperations();
			eso.setExpressionSet(formulaSet);

		} catch (Exception e) {
			throw new InterruptedException("Reducer initialisation error: " + e.getMessage());
		}

		// TODO load
		settings = new ExecutorSettings();
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}

	/**
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {

			Tuple keyTuple = null;

			if (!settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
				keyTuple = new Tuple(key);
			}

			/* create boolean value mapping */
			GFBooleanMapping mapGFtoB = eso.getBooleanMapping();
			BEvaluationContext booleanContext = new BEvaluationContext();

			for (Text v : values) {

				// if tuple pointer optimization is on
				// we need to find the actual tuple between the values.
				if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
					
					String val = v.toString();
					// if this is a tuple instead of an id
					if ( isTuple(val) ) {
						
						// extract the tuple
						keyTuple = getTuple(val);
						
						// skip id processing
						continue;
					}
				}

				int id = Integer.parseInt(v.toString());


				// skip empty values (only used for propagation)
				//				if (t.getLength() == 0)
				//					continue;

				// atoms that are present are "true"
				// id mode vs string mode
				if (receiveIDs) {
					GFAtomicExpression atom = eso.getAtom(id); 
					booleanContext.setValue(mapGFtoB.getVariable(atom), true);
				} else {
					Text t = null; // CLEAN
					Tuple tuple = new Tuple(t);
					GFAtomicExpression dummy = new GFAtomicExpression(tuple.getName(), tuple.getAllData());
					booleanContext.setValue(mapGFtoB.getVariable(dummy), true);
				}
			}

			// if key tuple is not present yet, throw exception
			if (keyTuple == null) {
				throw new GuardTupleNotFoundException("There was no guard tuple found for key "+ key.toString());
			}

			/* evaluate all formulas */
			for (GFExistentialExpression formula : eso.getExpressionSet()) {

				// only if applicable
				GFAtomicExpression guard = formula.getGuard();
				if (guard.matches(keyTuple)) {

					// get associated boolean expression
					BExpression booleanChildExpression = eso.getBooleanChildExpression(formula);

					// evaluate
					boolean eval = booleanChildExpression.evaluate(booleanContext);
					if (eval) {

						// determine output
						GFAtomProjection p = eso.getOutputProjection(formula);
						String outfile = generateFileName(p.getOutputSchema());

						// project the tuple and output it
						String outputTuple = p.project(keyTuple).generateString();
						out1.set(outputTuple);
						mos.write((Text)null, out1, outfile);
						context.getCounter(GumboRed2Counter.RED2_OUT_BYTES).increment(out1.getLength());
						context.getCounter(GumboRed2Counter.RED2_OUT_RECORDS).increment(1);
					}
				}

			}

		} catch (VariableNotFoundException | NonMatchingTupleException | GFOperationInitException | GuardTupleNotFoundException e) {
			// should not happen
			LOG.error("Unexpected exception: " + e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 
	}

	/**
	 * Extracts the guard tuple from a string. The String must have the form "#R(1,2,...,n)" (without quotes).
	 * @param val
	 * @return
	 */
	private Tuple getTuple(String val) {
		return new Tuple(val.substring(1));
	}

	/**
	 * Checks id the strings contains a guard tuple
	 * @param val
	 * @return
	 */
	private boolean isTuple(String val) {
		if (val.length() > 0)
			return val.charAt(0) == '#'; // TODO put this in settings
		return false;
	}

	public String generateFileName(RelationSchema relationSchema) {

		// cached?
		if (filenames.containsKey(relationSchema)) {
			return filenames.get(relationSchema);

		} else {
			String rel = relationSchema.getShortDescription();
			String name = generateFolder(relationSchema) + "/" + rel;
			filenames.put(relationSchema,name);
			return name;
		}

	}

	public String generateFolder(RelationSchema relationSchema) {
		return relationSchema.getShortDescription();
	}

}
