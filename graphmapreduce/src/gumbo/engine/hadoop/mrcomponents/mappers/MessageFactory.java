package gumbo.engine.hadoop.mrcomponents.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class MessageFactory {

	Text keyText;
	Text valueText;
	private Counter KAR;
	private Counter KARB;
	private Counter R;
	private Counter RB;
	private Counter RVB;
	private Counter RKB;
	private Counter KAPOE;
	private Counter KAPOEB;
	private boolean guardTuplePointerOptimizationOn;
	private boolean guardKeepaliveOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	private Mapper<LongWritable, Text, Text, Text>.Context context;

	// components
	private TupleIDCreator pathids;
	private ExpressionSetOperations eso;
	private StringBuilder keyBuilder;
	private StringBuilder valueBuilder;

	// data
	Tuple t;
	String tRef;
	String proofBytes;

	public MessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		this.eso = eso;
		this.pathids = new TupleIDCreator(eso.getFileMapping());
		keyBuilder = new StringBuilder(16);
		valueBuilder = new StringBuilder(128);

		// ---
		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn);
		guardKeepaliveOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepaliveOptimizationOn);
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);

		// ---
		KAR = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST);
		KARB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_BYTES);

		R = context.getCounter(GumboMap1Counter.REQUEST);
		RB = context.getCounter(GumboMap1Counter.REQUEST_BYTES);
		RVB = context.getCounter(GumboMap1Counter.REQUEST_KEY_BYTES);
		RKB = context.getCounter(GumboMap1Counter.REQUEST_VALUE_BYTES);


		KAPOE = context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE);
		KAPOEB = context.getCounter(GumboMap1Counter.KEEP_ALIVE_PROOF_OF_EXISTENCE_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


	}

	public void loadGuardValue(Tuple t, long offset) throws TupleIDError {

		this.t = t;
		// replace value with pointer when optimization is on
		if (guardTuplePointerOptimizationOn) {
			tRef = pathids.getTupleID(context, offset); // key indicates offset in TextInputFormat
		} else
			tRef = t.toString();

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
	public void sendGuardKeepAliveRequest(GFAtomicExpression guard) throws IOException, InterruptedException, GFOperationInitException {

		// TODO enable id opt.
		int guardID = eso.getAtomId(guard);

		// output guard
		if (!guardKeepaliveOptimizationOn) {
			keyBuilder.append(t.toString());

			valueBuilder.append(tRef);
			valueBuilder.append(';');
			valueBuilder.append(guardID);

			KAR.increment(1);
			KARB.increment(keyBuilder.length() + valueBuilder.length());
			
			sendMessage();
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
	public void sendGuardedAssert(boolean force) throws IOException, InterruptedException {

		if (!guardKeepaliveOptimizationOn || force) {

			keyBuilder.append(t.toString());

			// add special 
			if (round1FiniteMemoryOptimizationOn)
				keyBuilder.append(proofBytes);

			// TODO proof representation optimization

			valueBuilder.append(proofBytes);

			// TODO count
			

			sendMessage();
		}

	}

	public void sendRequest(Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo) throws IOException, InterruptedException, NonMatchingTupleException {

		//		GFAtomicExpression guarded = guardedInfo.fst;
		GFAtomProjection p = guardedInfo.snd;
		Integer guardedID = guardedInfo.trd;

		keyBuilder.append(p.projectString(t));

		// value: request message with response code and atom
		//		String valueString = replyAddress + ";" + guardedID;

		valueBuilder.append(tRef);
		valueBuilder.append(';');
		valueBuilder.append(guardedID.toString());

		R.increment(1);
		RB.increment(keyBuilder.length() + valueBuilder.length());
		RKB.increment(keyBuilder.length());
		RVB.increment(valueBuilder.length());

		sendMessage();
	}

	protected void sendMessage() throws IOException, InterruptedException{
		sendMessage(keyBuilder.toString().getBytes(),valueBuilder.toString().getBytes());
		keyBuilder.setLength(0);
		valueBuilder.setLength(0);
	}


	protected void sendMessage(byte[] key, byte[] value) throws IOException, InterruptedException {
		keyText.clear();
		valueText.clear();
		keyText.append(key, 0, key.length);
		valueText.append(value, 0, value.length);
		
		context.write(keyText, valueText);
		
		//System.out.println("<" +keyText.toString()+ " : " + valueText.toString() + ">");
	}

}
