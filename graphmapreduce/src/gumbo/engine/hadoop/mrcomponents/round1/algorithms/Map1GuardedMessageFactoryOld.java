package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.mappers.GumboMap1Counter;
import gumbo.engine.hadoop.reporter.CounterMeasures;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;


public class Map1GuardedMessageFactoryOld implements Map1GuardedMessageFactoryInterface {

	private static final Log LOG = LogFactory.getLog(Map1GuardedMessageFactoryOld.class);

	private Text keyText;
	private Text valueText;
	private Counter ASSERT;
	private Counter ASSERTBYTES;


	private boolean sampleCounter;

	private boolean guardedIdOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	private boolean mapOutputGroupingOptimizationOn;

	private Mapper<LongWritable, Text, Text, Text>.Context context;

	// components
	private StringBuilder keyBuilder;
	private StringBuilder valueBuilder;

	// data
	Tuple t;
	String tKey;
	String proofBytes;

	public Map1GuardedMessageFactoryOld(Mapper<LongWritable, Text, Text, Text>.Context context, AbstractExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		keyBuilder = new StringBuilder(32);
		valueBuilder = new StringBuilder(128);

		// ---
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
		guardedIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn);
		mapOutputGroupingOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.mapOutputGroupingOptimizationOn);

		// ---

		ASSERT = context.getCounter(GumboMap1Counter.ASSERT);
		ASSERTBYTES = context.getCounter(GumboMap1Counter.ASSERT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


		// ---

		// prepare the value with the proof symbol
		if (guardedIdOptimizationOn)
			valueBuilder.append(proofBytes);

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
		keyBuilder.setLength(0);
		keyBuilder.append(t.generateString(mapOutputGroupingOptimizationOn));

		// add sort indication to key if necessary
		if (round1FiniteMemoryOptimizationOn) {
			keyBuilder.append(proofBytes);
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
			valueBuilder.setLength(0);
			valueBuilder.append(t.toString());
		}

		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyBuilder.length()+valueBuilder.length());

		sendMessage();



	}



	protected void sendMessage() throws MessageFailedException {
		sendMessage(keyBuilder.toString().getBytes(),valueBuilder.toString().getBytes());
	}


	protected void sendMessage(byte[] key, byte[] value) throws MessageFailedException {
		try {
			// OPTIMIZE is it better to work directly on the Text objects?
			keyText.clear();
			valueText.clear();
			keyText.append(key, 0, key.length);
			valueText.append(value, 0, value.length);

			context.write(keyText, valueText);
//			LOG.info("ASSERT: " + keyText + " : " + valueText );


			if (sampleCounter) {
				context.getCounter(CounterMeasures.OUT_TUPLES).increment(1);
				context.getCounter(CounterMeasures.OUT_BYTES).increment(key.length + value.length);
				context.getCounter(CounterMeasures.OUT_KEY_BYTES).increment(key.length);
				context.getCounter(CounterMeasures.OUT_VALUE_BYTES).increment(value.length);
			}


			//System.out.println("<" +keyText.toString()+ " : " + valueText.toString() + ">");
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactoryInterface#sendAssert(java.util.Set)
	 */
	@Override
	public void sendAssert(Set<Integer> ids) throws MessageFailedException {
		valueBuilder.setLength(0);
		if (!guardedIdOptimizationOn) {
			valueBuilder.append(t.toString());
		} else {
			valueBuilder.append(proofBytes);
		}

		if (mapOutputGroupingOptimizationOn)
			for(int id : ids) {
				valueBuilder.append(":");
				valueBuilder.append(id);
			}

		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyBuilder.length()+valueBuilder.length());

		sendMessage();

	}

}
