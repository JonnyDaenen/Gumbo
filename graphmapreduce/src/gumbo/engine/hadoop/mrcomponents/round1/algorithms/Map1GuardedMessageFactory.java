package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GumboMap1Counter;
import gumbo.engine.hadoop.reporter.CounterMeasures;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;


public class Map1GuardedMessageFactory implements Map1GuardedMessageFactoryInterface {

	private static final Log LOG = LogFactory.getLog(Map1GuardedMessageFactory.class);

	private Text keyText;
	private Text valueText;
	private Counter ASSERT;
	private Counter ASSERTBYTES;


	private boolean sampleCounter;

	private boolean guardedIdOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	private boolean mapOutputGroupingOptimizationOn;

	private Mapper<LongWritable, Text, Text, Text>.Context context;


	// data
	Tuple t;
	String tKey;
	byte [] proofBytes;

	private byte[] separatorBytes;

	public Map1GuardedMessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, AbstractExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;

		// ---
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
		guardedIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn);
		mapOutputGroupingOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.mapOutputGroupingOptimizationOn);

		// ---

		ASSERT = context.getCounter(GumboMap1Counter.ASSERT);
		ASSERTBYTES = context.getCounter(GumboMap1Counter.ASSERT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL).getBytes();
		separatorBytes = ":".getBytes();


		// ---

		// prepare the value with the proof symbol
		if (guardedIdOptimizationOn)
			valueText.set(settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL));

		sampleCounter = false;
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactoryInterface#enableSampleCounting()
	 */
	@Override
	public void enableSampleCounting() {
		sampleCounter = true;
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactoryInterface#loadGuardedValue(gumbo.structures.data.Tuple)
	 */
	@Override
	public void loadGuardedValue(Tuple t) {

		this.t = t;
		
		keyText.set(t.generateString(mapOutputGroupingOptimizationOn));

		// add sort indication to key if necessary
		if (round1FiniteMemoryOptimizationOn) {
			keyText.append(proofBytes, 0, proofBytes.length);
		} 

		if (sampleCounter) {
			context.getCounter(CounterMeasures.IN_TUPLES).increment(1);
			context.getCounter(CounterMeasures.IN_BYTES).increment(t.toString().length());
		}
	}



	
	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactoryInterface#sendAssert()
	 */
	@Override
	public void sendAssert() throws MessageFailedException {

		if (!guardedIdOptimizationOn) {
			valueText.set(t.toString());
		}

		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyText.getLength()+valueText.getLength());

		sendMessage();



	}



	protected void sendMessage() throws MessageFailedException {
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



	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactoryInterface#sendAssert(java.util.Set)
	 */
	@Override
	public void sendAssert(Set<Integer> ids) throws MessageFailedException {

		if (!guardedIdOptimizationOn) {
			valueText.set(t.toString());
		} else {
			valueText.set(proofBytes,0,proofBytes.length);
		}

		if (mapOutputGroupingOptimizationOn)
			for(int id : ids) {
				valueText.append(separatorBytes, 0, separatorBytes.length);
				String repr = Integer.toString(id);
				byte [] reprbytes = repr.getBytes();
				valueText.append(reprbytes,0,reprbytes.length);
			}

		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyText.getLength()+valueText.getLength());

		sendMessage();

	}

}
