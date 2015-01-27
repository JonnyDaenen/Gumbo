/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.ExecutorSettings;
import mapreduce.guardedfragment.executor.hadoop.mappers.TupleIDCreator.TupleIDError;
import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;
import mapreduce.utils.LongBase64Converter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
	 * @see mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(rm);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
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
			if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
				value.set(pathids.getTupleID(context, key.get())); // key indicates offset in TextInputFormat
			}
			// System.out.println(t);


			boolean outputGuard = false;
			boolean guardIsGuarded = false;
			
			LOG.error("tuple:" + t);

			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				LOG.error("guard:" + guard);
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guard
					if (!settings.getBooleanProperty(ExecutorSettings.guardKeepaliveOptimizationOn)) {
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
						LOG.error("Guard schema" + guard.getRelationSchema());
						LOG.error("Guarded schema" + guarded.getRelationSchema());
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
			if (!settings.getBooleanProperty(ExecutorSettings.guardKeepaliveOptimizationOn) && outputGuard) {
				context.write(value, value);
				context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE).increment(1);
				context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE_BYTES).increment(value.getLength()*2);
				//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
			}
			
			// output a proof of existence if the guard is also guarded
			if (guardIsGuarded) {
				LOG.error("guard output POE");
				out1.set(t.toString());
				out2.set(settings.getProperty(ExecutorSettings.PROOF_SYMBOL));
				context.write(out1, out2);
			}
			
			

		} catch (SecurityException | TupleIDError | NonMatchingTupleException | GFOperationInitException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

	


}
