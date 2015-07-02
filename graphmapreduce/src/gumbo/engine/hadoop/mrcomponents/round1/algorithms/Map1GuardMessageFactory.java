package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.general.messagefactories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GumboMap1Counter;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.reporter.CounterMeasures;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1GuardMessageFactory implements Map1GuardMessageFactoryInterface {


	private static final Log LOG = LogFactory.getLog(Map1GuardMessageFactory.class);

	Text keyText;
	Text valueText;
	private Counter KAR;
	private Counter KARB;
	private Counter R;
	private Counter RB;
	private Counter RVB;
	private Counter RKB;
	private Counter ASSERT;
	private Counter ASSERTBYTES;
	private boolean guardTuplePointerOptimizationOn;
	private boolean guardKeepaliveOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	private boolean guardIdOptimizationOn;
	private boolean guardedIdOptimizationOn;
	private boolean mapOutputGroupingOptimizationOn;
	private Mapper<LongWritable, Text, Text, Text>.Context context;

	// components
	private TupleIDCreator pathids;
	private ExpressionSetOperations eso;
	//	private StringBuilder keyBuilder;
	//	private StringBuilder valueBuilder;

	// data
	Tuple t;
	byte [] tRef;
	byte [] proofBytes;
	byte [] tbytes;
	private byte[] sepBytes;
	private byte [] commaBytes;
	private boolean sampleCounter;

	private Map<String, Set<String>> messageBuffer;

	public Map1GuardMessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, AbstractExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();
		// ---
		this.context = context;
		this.eso = eso;
		this.pathids = new TupleIDCreator(eso.getFileMapping());
		//		keyBuilder = new StringBuilder(32);
		//		valueBuilder = new StringBuilder(128);

		// ---
		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn);
		guardKeepaliveOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveOptimizationOn);
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
		guardIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.requestAtomIdOptimizationOn);
		guardedIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn);
		mapOutputGroupingOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.mapOutputGroupingOptimizationOn);

		// ---
		KAR = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST);
		KARB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_BYTES);

		R = context.getCounter(GumboMap1Counter.REQUEST);
		RB = context.getCounter(GumboMap1Counter.REQUEST_BYTES);
		RVB = context.getCounter(GumboMap1Counter.REQUEST_KEY_BYTES);
		RKB = context.getCounter(GumboMap1Counter.REQUEST_VALUE_BYTES);


		ASSERT = context.getCounter(GumboMap1Counter.KEEP_ALIVE_ASSERT);
		ASSERTBYTES = context.getCounter(GumboMap1Counter.KEEP_ALIVE_ASSERT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL).getBytes();
		sepBytes = ";".getBytes();
		commaBytes = ":".getBytes();

		sampleCounter = false;

		messageBuffer = new HashMap<>();
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#enableSampleCounting()
	 */
	@Override
	public void enableSampleCounting() {
		sampleCounter = true;
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#loadGuardValue(gumbo.structures.data.Tuple, long)
	 */
	@Override
	public void loadGuardValue(Tuple t, long offset) throws TupleIDError {

		this.t = t;
		tbytes = t.toString().getBytes();
		// replace value with pointer when optimization is on
		if (guardTuplePointerOptimizationOn) {
			tRef = pathids.getTupleID(context, offset).getBytes(); // key indicates offset in TextInputFormat
		} else
			tRef = tbytes;

		if (sampleCounter) {
			context.getCounter(CounterMeasures.IN_TUPLES).increment(1);
			context.getCounter(CounterMeasures.IN_BYTES).increment(t.toString().length());
		}
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#sendGuardKeepAliveRequest(gumbo.structures.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public void sendGuardKeepAliveRequest(GFAtomicExpression guard) throws MessageFailedException {

		try {
			// CLEAN duplicate code, reuse standard request message
			byte [] guardRef;
			if (guardIdOptimizationOn)
				guardRef = Integer.toString(eso.getAtomId(guard)).getBytes();
			else
				guardRef = guard.toString().getBytes();

			// output guard
			if (!guardKeepaliveOptimizationOn) {

				byte[] tbytes2 = t.generateString(mapOutputGroupingOptimizationOn).getBytes();
				keyText.append(tbytes2,0,tbytes2.length);

				valueText.append(tRef,0,tRef.length);
				valueText.append(sepBytes,0,sepBytes.length);
				valueText.append(guardRef,0,guardRef.length);

				KAR.increment(1);
				KARB.increment(keyText.getLength() + valueText.getLength());

				sendMessage();
			}
		} catch(GFOperationInitException e) {
			throw new MessageFailedException(e);
		}
	}


	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#sendGuardedAssert(boolean, java.util.Set)
	 */
	@Override
	public void sendGuardedAssert(boolean force, Set<Integer> ids) throws MessageFailedException {

		if (!guardKeepaliveOptimizationOn || force) {

			// possibly the key has to be unwrapped
			byte[] tbytes2 = t.generateString(mapOutputGroupingOptimizationOn).getBytes();
			keyText.append(tbytes2,0,tbytes2.length);

			// add special symbol for sort order
			if (round1FiniteMemoryOptimizationOn)
				keyText.append(proofBytes,0,proofBytes.length);

			// proof representation optimization
			if (guardedIdOptimizationOn)
				valueText.append(proofBytes,0,proofBytes.length);
			else
				valueText.append(tbytes,0,tbytes.length);

			if (mapOutputGroupingOptimizationOn) {
				for (int id : ids) {
					valueText.append(commaBytes,0,commaBytes.length);
					byte[] valBytes = (""+id).getBytes();
					valueText.append(valBytes,0,valBytes.length);
				}
			}

			// update counters
			ASSERT.increment(1);
			ASSERTBYTES.increment(keyText.getLength() + valueText.getLength());

			sendMessage();
		}

	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#sendRequest(gumbo.structures.gfexpressions.io.Triple)
	 */
	@Override
	public void sendRequest(Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo) throws MessageFailedException {

		try {
			//		GFAtomicExpression guarded = guardedInfo.fst;
			GFAtomProjection p = guardedInfo.snd;



			String keyString = p.projectString(t,mapOutputGroupingOptimizationOn);
			String guardedString;
			if (guardIdOptimizationOn)
				guardedString = Integer.toString(guardedInfo.trd);
			else
				guardedString = guardedInfo.fst.toString();


			// if necessary, buffer for later grouping
			if (mapOutputGroupingOptimizationOn) {

				if (!messageBuffer.containsKey(keyString)) {
					messageBuffer.put(keyString, new HashSet<String>(10));
				}

				messageBuffer.get(keyString).add(guardedString);

			} else {
				byte [] guardedRef = guardedString.getBytes();


				byte[] projectBytes = keyString.getBytes();
				keyText.append(projectBytes,0,projectBytes.length);


				// value: request message with response code and atom
				//		String valueString = replyAddress + ";" + guardedID;


				valueText.append(tRef,0,tRef.length);
				valueText.append(sepBytes,0,sepBytes.length);
				valueText.append(guardedRef,0,guardedRef.length);

				R.increment(1);
				RB.increment(keyText.getLength() + valueText.getLength());
				RKB.increment(keyText.getLength());
				RVB.increment(valueText.getLength());

				sendMessage();
			}
		} catch(NonMatchingTupleException e) {
			throw new MessageFailedException(e);
		}
	}

	protected void sendMessage() throws MessageFailedException{
		try {

			context.write(keyText, valueText);

//			LOG.info("<" + keyText.toString() + " : " + valueText.toString() + ">");

			if (sampleCounter) {
				context.getCounter(CounterMeasures.OUT_TUPLES).increment(1);
				context.getCounter(CounterMeasures.OUT_BYTES).increment(keyText.getLength() + valueText.getLength());
				context.getCounter(CounterMeasures.OUT_KEY_BYTES).increment(keyText.getLength());
				context.getCounter(CounterMeasures.OUT_VALUE_BYTES).increment(valueText.getLength());
			}

			keyText.clear();
			valueText.clear();



		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}

	private static byte[] getBytesFast(StringBuilder builder) {
		final int length = builder.length(); 

		final char buffer[] = new char[length];
		builder.getChars(0, length, buffer, 0);

		final byte b[] = new byte[length];
		for (int j = 0; j < length; j++)
			b[j] = (byte) buffer[j];

		return b;
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactoryInterface#finish()
	 */
	@Override
	public void finish() throws MessageFailedException {

		// group buffered messages on their key
		for ( String key: messageBuffer.keySet() ){

			keyText.set(key);

			valueText.set(tRef);
			valueText.append(sepBytes,0,sepBytes.length);
			for (String val : messageBuffer.get(key)) {
				byte [] bytes = val.getBytes();
				valueText.append(bytes, 0, bytes.length);
				valueText.append(commaBytes, 0, commaBytes.length);
			}

			// update counters
			R.increment(1);
			RB.increment(keyText.getLength() + valueText.getLength());
			RKB.increment(keyText.getLength());
			RVB.increment(valueText.getLength());

			// send them out
			sendMessage();
		}




		messageBuffer.clear();
	}




}
