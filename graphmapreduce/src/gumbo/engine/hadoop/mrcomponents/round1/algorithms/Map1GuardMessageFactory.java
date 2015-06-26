package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.mappers.GumboMap1Counter;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.reporter.CounterMeasures;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1GuardMessageFactory {

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
	private boolean sampleCounter;

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

		sampleCounter = false;
	}

	public void enableSampleCounting() {
		sampleCounter = true;
	}

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

	/**
	 * Sends a Guard keep-alive request message.
	 * The key of the message is the tuple itself,
	 * in string representation. The value
	 * consists of a reply address and a reply value.
	 * The reply address is a reference to the tuple,
	 * the reply value is a reference to a guarded atom.
	 * Both representations can be an id, or the object 
	 * in string representation, depending on the 
	 * optimization settings.
	 * 
	 * @param guard
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws GFOperationInitException
	 */
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
				keyText.append(tbytes,0,tbytes.length);

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


	/**
	 * Sends out an assert message to the guard tuple,
	 * as part of the Keep-alive system.
	 * The key is the tuple itself, in string representation.
	 * The value consists of a reference to the tuple,
	 * which can either be the tuple itself, or an id
	 * representing the tuple. This is dependent of the
	 * optimization settings.
	 * 
	 * When finite memory optimization is enabled,
	 * the key is padded with a special symbol in order
	 * to sort, partition and group correctly.
	 * 
	 * <b>Important:</b> when keep-alive optimization is enabled, 
	 * no message will be sent, unless force is true.
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendGuardedAssert(boolean force) throws MessageFailedException {

		if (!guardKeepaliveOptimizationOn || force) {

			keyText.append(tbytes,0,tbytes.length);

			// add special symbol for sort order
			if (round1FiniteMemoryOptimizationOn)
				keyText.append(proofBytes,0,proofBytes.length);

			// proof representation optimization
			if (guardedIdOptimizationOn)
				valueText.append(proofBytes,0,proofBytes.length);
			else
				valueText.append(tbytes,0,tbytes.length);

			// update counters
			ASSERT.increment(1);
			ASSERTBYTES.increment(keyText.getLength() + valueText.getLength());

			sendMessage();
		}

	}

	public void sendRequest(Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo) throws MessageFailedException {

		try {
			//		GFAtomicExpression guarded = guardedInfo.fst;
			GFAtomProjection p = guardedInfo.snd;

			byte [] guardRef;
			if (guardIdOptimizationOn)
				guardRef = Integer.toString(guardedInfo.trd).getBytes();
			else
				guardRef = guardedInfo.fst.toString().getBytes();

			byte[] projectBytes = p.projectString(t).getBytes();
			keyText.append(projectBytes,0,projectBytes.length);


			// value: request message with response code and atom
			//		String valueString = replyAddress + ";" + guardedID;

			valueText.append(tRef,0,tRef.length);
			valueText.append(sepBytes,0,sepBytes.length);
			valueText.append(guardRef,0,guardRef.length);

			R.increment(1);
			RB.increment(keyText.getLength() + valueText.getLength());
			RKB.increment(keyText.getLength());
			RVB.increment(valueText.getLength());

			sendMessage();
		} catch(NonMatchingTupleException e) {
			throw new MessageFailedException(e);
		}
	}

	protected void sendMessage() throws MessageFailedException{
		try {

			context.write(keyText, valueText);

			//		System.out.println("<" +keyText.toString()+ " : " + valueText.toString() + ">");

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




}
