package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.mappers.GumboMap1Counter;
import gumbo.engine.hadoop.reporter.CounterMeasures;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;


public class Map1GuardedMessageFactory {

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

	// components
	private StringBuilder keyBuilder;
	private StringBuilder valueBuilder;

	// data
	Tuple t;
	String tKey;
	String proofBytes;

	public Map1GuardedMessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, AbstractExecutorSettings settings, ExpressionSetOperations eso) {
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

	public void enableSampleCounting() {
		sampleCounter = true;
	}

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
			LOG.info("ASSERT: " + keyText + " : " + valueText );


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

	/**
	 * Sends out an assert message to this guarded tuple,
	 * to indicate its own existance.
	 * 
	 * If the guarded ID optimization is on,
	 * the message constant is replaced with a special constant symbol.
	 * 
	 * If map output grouping is on, the atom ids of matching
	 * atoms are also sent as a list. Note that, if the supplied list is empty,
	 * this methods acts as if grouping is off and just sends the 
	 * message. This means that checking if the message should be sent
	 * should be done before calling this method.
	 * 
	 * @param ids the list of atom ids that match the tuple
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendAssert(Set<Integer> ids) throws MessageFailedException {
		valueBuilder.setLength(0);
		if (!guardedIdOptimizationOn) {
			valueBuilder.append(t.toString());
		} else {
			valueBuilder.append(proofBytes);
		}

		if (mapOutputGroupingOptimizationOn)
			for(int id : ids) {
				valueBuilder.append(",");
				valueBuilder.append(id);
			}

		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyBuilder.length()+valueBuilder.length());

		sendMessage();

	}

}
