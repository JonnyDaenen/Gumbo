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
import org.apache.hadoop.mapreduce.Counter;

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
	Text proofText;
	TupleIDCreator pathids; // OPTIMIZE extract this and perform outside of mapper

	/* settings cache */
	boolean guardTuplePointerOptimizationOn;
	boolean guardKeepaliveOptimizationOn;
	boolean round1FiniteMemoryOptimizationOn;

	protected Counter KAR;
	protected Counter KARB;
	

	protected Counter R;
	protected Counter RB;
	protected Counter RVB;
	protected Counter RKB;
	

	protected Counter KAPOE;
	protected Counter KAPOEB;

	private StringBuffer buffer;


	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(eso.getFileMapping());

		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn);
		guardKeepaliveOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn);
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
	
		proofText = new Text(settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL));
		
		KAR = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST);
		KARB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_BYTES);
		
		

		
		R = context.getCounter(GumboMap1Counter.REQUEST);
		RB = context.getCounter(GumboMap1Counter.REQUEST_BYTES);
		RVB = context.getCounter(GumboMap1Counter.REQUEST_KEY_BYTES);
		RKB = context.getCounter(GumboMap1Counter.REQUEST_VALUE_BYTES);
		

		KAPOE = context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE);
		KAPOEB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE_BYTES);
		
		buffer = new StringBuffer(128);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		// trim is necessary to remove extra whitespace
//		value.set(value.toString().trim());

		

		try {

			// transform data
			Tuple t = new Tuple(value.getBytes());
			String replyAddress;

			// replace value with pointer when optimization is on
			if (guardTuplePointerOptimizationOn) {
				replyAddress = pathids.getTupleID(context, key.get()); // key indicates offset in TextInputFormat
			} else
				replyAddress = t.toString();
				


			boolean outputGuard = false;
			boolean guardIsGuarded = false;


			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guard
					if (!guardKeepaliveOptimizationOn) {
						buffer.setLength(0);
						buffer.append(replyAddress);
						buffer.append(';');
						buffer.append(guardID);
//						String valueString = replyAddress + ";" + guardID;
						out1.set(buffer.toString().getBytes());
						context.write(value, out1); // TODO is this ok for pointers?
						KAR.increment(1);
						KARB.increment(out1.getLength() + value.getLength());
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
						out1.set(tprime.toString().getBytes());

						// value: request message with response code and atom
//						String valueString = replyAddress + ";" + guardedID;
						buffer.setLength(0);
						buffer.append(replyAddress);
						buffer.append(';');
						buffer.append(guardedID);
						out2.set(buffer.toString().getBytes());

						context.write(out1, out2);
						R.increment(1);
						RB.increment(out1.getLength() + out2.getLength());
						RKB.increment(out1.getLength());
						RVB.increment(out2.getLength());

						//							LOG.warn("Guard: " + out1 + out2);
						//						}

					}
				}
			}

			// only output keep-alive if it matched a guard
			if (!guardKeepaliveOptimizationOn && outputGuard) {
				context.write(value, value); // TODO pointers/POE?
				KAPOE.increment(1);
				KAPOEB.increment(value.getLength()*2);
				//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
			}

			// output a proof of existence if the guard is also guarded
			if (guardIsGuarded) {
				//				LOG.error("guard output POE");
				String proofSymbol = "";
				if (round1FiniteMemoryOptimizationOn) {
					proofSymbol = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);
				}
				out1.set((t.toString() + proofSymbol).getBytes());
				context.write(out1, proofText);
				// TODO count
			}



		} catch (SecurityException | TupleIDError | NonMatchingTupleException | GFOperationInitException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}




}
