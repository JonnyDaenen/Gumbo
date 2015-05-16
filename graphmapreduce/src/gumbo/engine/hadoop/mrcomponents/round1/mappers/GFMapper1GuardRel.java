/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.io.Triple;
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
	byte [] commaBytes = {';'};
	byte [] proofBytes = {'#'};
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



	/**
	 * @see gumbo.engine.hadoop.mrcomponents.round1.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(eso.getFileMapping());

		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveReductionOn);
		guardKeepaliveOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveReductionOn);
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
	
		proofText = new Text(settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL));
		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL).getBytes();
		
		KAR = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST);
		KARB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_BYTES);
		
		

		
		R = context.getCounter(GumboMap1Counter.REQUEST);
		RB = context.getCounter(GumboMap1Counter.REQUEST_BYTES);
		RVB = context.getCounter(GumboMap1Counter.REQUEST_KEY_BYTES);
		RKB = context.getCounter(GumboMap1Counter.REQUEST_VALUE_BYTES);
		

		KAPOE = context.getCounter(GumboMap1Counter.KEEP_ALIVE_ASSERT);
		KAPOEB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_ASSERT_BYTES);
		
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
			Tuple t = new Tuple(value.getBytes(),value.getLength());
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
						String valueString = replyAddress + ";" + guardID;
						out1.set(valueString.getBytes());
						
						context.write(value, out1); // TODO is this ok for pointers?
						KAR.increment(1);
						KARB.increment(out1.getLength() + value.getLength());
					}
					// LOG.warn(value.toString() + " " + out1.toString());
					outputGuard = true;

					// projections to atoms
					for (Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo : eso.getGuardedsAndProjections(guard)) {

						GFAtomicExpression guarded = guardedInfo.fst;
						GFAtomProjection p = guardedInfo.snd;
						Integer guardedID = guardedInfo.trd;
						
						// TODO if guarded is same relation, output proof of existence afterwards
						if (guarded.getRelationSchema().equals(guard.getRelationSchema())) {
							guardIsGuarded = true;
						}

//						GFAtomProjection p = eso.getProjections(guard, guarded);
//						Tuple tprime = p.project(t);


//						int guardedID = eso.getAtomId(guarded);

						// if the guard projects to this guarded
						// TODO isn't this always the case?
						//						if (guarded.matches(tprime)) {
						// key: the value we are looking for 
						out1.set(p.projectString(t).getBytes());

						// value: request message with response code and atom
//						String valueString = replyAddress + ";" + guardedID;
						byte [] replyBytes = replyAddress.getBytes();
						byte [] IDBytes = guardedID.toString().getBytes();
						out2.clear();
						out2.append(replyBytes,0,replyBytes.length);
						out2.append(commaBytes,0,commaBytes.length);
						out2.append(IDBytes,0,IDBytes.length);
//						out2.set(valueString.getBytes());

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

				
				byte[] replyBytes = t.toString().getBytes();
				out1.clear();
				out1.append(replyBytes,0,replyBytes.length);
				if (round1FiniteMemoryOptimizationOn) {
//					proofSymbol = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);
					out1.append(proofBytes,0,proofBytes.length);
				}
				
//				out1.set((t.toString() + proofSymbol).getBytes());
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
