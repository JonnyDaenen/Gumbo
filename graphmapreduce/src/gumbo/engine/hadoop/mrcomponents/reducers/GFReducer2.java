/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.reducers;

import gumbo.engine.hadoop.mrcomponents.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.VariableNotFoundException;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Uses atom data generated by the corresponding mapper.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFReducer2 extends Reducer<Text, IntWritable, Text, Text> {


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


	private static final Log LOG = LogFactory.getLog(GFReducer2.class);

	boolean receiveIDs = true;
	private HadoopExecutorSettings settings;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();

		mos = new MultipleOutputs<>(context);

		String s = String.format("Reducer"+this.getClass().getSimpleName()+"-%05d-%d",
				context.getTaskAttemptID().getTaskID().getId(),
				context.getTaskAttemptID().getId());
		LOG.info(s);

		// load parameters
		try {
			ParameterPasser pp = new ParameterPasser(conf);
			eso = pp.loadESO();
			settings = pp.loadSettings();
		} catch (Exception e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}
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
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		try {

			Tuple keyTuple = null;

			if (!settings.getBooleanProperty(HadoopExecutorSettings.guardTuplePointerOptimizationOn)) {
				keyTuple = new Tuple(key);
			}

			/* create boolean value mapping */
			GFBooleanMapping mapGFtoB = eso.getBooleanMapping();
			BEvaluationContext booleanContext = new BEvaluationContext();

			for (IntWritable v : values) {

				// if tuple pointer optimization is on
				// we need to find the actual tuple between the values.
				if (settings.getBooleanProperty(HadoopExecutorSettings.guardTuplePointerOptimizationOn)) {
					// TODO implement...
				}

				int id = v.get();


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
				throw new GuardTupleNotFoundException("THere was no guard tuple found for key "+ key.toString());
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

	public String generateFileName(RelationSchema rs) {
		
		
		Set<Path> paths = eso.getFileMapping().getPaths(rs);
		// take first path
		for (Path path: paths) {
			return path.toString() + "/" + rs.getName();
		}
		return ""; // FIXME fallback system + duplicate code in other reducer2

//		// cached?
//		if (filenames.containsKey(relationSchema)) {
//			return filenames.get(relationSchema);
//
//		} else {
//			String rel = relationSchema.getShortDescription();
//			String name = generateFolder(relationSchema) + "/" + rel;
//			filenames.put(relationSchema,name);
//			return name;
//		}

	}

	public String generateFolder(RelationSchema relationSchema) {
		return relationSchema.getShortDescription();
	}

}
