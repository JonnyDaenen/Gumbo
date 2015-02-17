/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardRel extends GFMapper1Identity {



	private static final Log LOG = LogFactory.getLog(GFMapper1GuardRel.class);

	Text out1 = new Text();
	Text out2 = new Text();
	TupleIDCreator pathids; // OPTIMIZE extract this and perform outside of mapper


	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(eso.getFileMapping());
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		boolean print = false;
		if (value.toString().contains(",1000,")) {
			LOG.error("Mapper1: " + value);
			print = true;
		}
		// trim is necessary to remove extra whitespace
		value.set(value.toString().trim());

		try {



			Tuple t = new Tuple(value);

			// replace value with pointer when optimization is on
			if (settings.getBooleanProperty(HadoopExecutorSettings.guardTuplePointerOptimizationOn)) {
				value.set(pathids.getTupleID(context, key.get())); // key indicates offset in TextInputFormat
			}
			// System.out.println(t);


			boolean outputGuard = false;
			boolean guardIsGuarded = false;
			

			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guard
					if (!settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn)) {
						out1.set(value.toString() + ";" + guardID);
						context.write(value, out1);
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST).increment(1);
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_BYTES).increment(out1.getLength() + value.getLength());
					}
					// LOG.warn(value.toString() + " " + out1.toString());
					outputGuard = true;

					// projections to atoms
					for (GFAtomicExpression guarded : eso.getGuardeds(guard)) {

						// TODO if guarded is same relation, output proof of existence afterwards
						if (guarded.getRelationSchema().equals(guard.getRelationSchema())) {
							guardIsGuarded = true;
						}
						
						GFAtomProjection p = eso.getProjections(guard, guarded);
						Tuple tprime = p.project(t);


						int guardedID = eso.getAtomId(guarded);

						// if the guard projects to this guarded
						// TODO isn't this always the case?
						//						if (guarded.matches(tprime)) {
						// key: the value we are looking for 
						out1.set(tprime.toString());

						// value: request message with response code and atom
						out2.set(value + ";" + guardedID);

						context.write(out1, out2);
						context.getCounter(GumboMap1Counter.REQUEST).increment(1);
						context.getCounter(GumboMap1Counter.REQUEST_BYTES).increment(out1.getLength() + out2.getLength());
						context.getCounter(GumboMap1Counter.REQUEST_KEY_BYTES).increment(out1.getLength());
						context.getCounter(GumboMap1Counter.REQUEST_VALUE_BYTES).increment(out2.getLength());
						if (print) {
							LOG.error("Mapper1 output: " + out1 + " " + out2);
						}
						//							LOG.warn("Guard: " + out1 + out2);
						//						}

					}
				}
			}

			// only output keep-alive if it matched a guard
			if (!settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn) && outputGuard) {
				context.write(value, value);
				context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE).increment(1);
				context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE_BYTES).increment(value.getLength()*2);
				//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
			}
			
			// output a proof of existence if the guard is also guarded
			if (guardIsGuarded) {
//				LOG.error("guard output POE");
				out1.set(t.toString());
				out2.set(settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL));
				context.write(out1, out2);
			}
			
			

		} catch (SecurityException | TupleIDError | NonMatchingTupleException | GFOperationInitException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

	


}
