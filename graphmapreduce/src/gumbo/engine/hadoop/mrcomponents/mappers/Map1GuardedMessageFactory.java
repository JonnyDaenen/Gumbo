package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1GuardedMessageFactory {

	Text keyText;
	Text valueText;
	private Counter ASSERT;
	private Counter ASSERTBYTES;
	
	private boolean guardedIdOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	
	private Mapper<LongWritable, Text, Text, Text>.Context context;

	// components
	private StringBuilder keyBuilder;
	private StringBuilder valueBuilder;

	// data
	Tuple t;
	String tKey;
	String proofBytes;

	public Map1GuardedMessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		keyBuilder = new StringBuilder(32);
		valueBuilder = new StringBuilder(128);

		// ---
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
		guardedIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardedIdOptimizationOn);

		// ---

		ASSERT = context.getCounter(GumboMap1Counter.ASSERT);
		ASSERTBYTES = context.getCounter(GumboMap1Counter.ASSERT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


		// ---
		
		// prepare the value with the proof symbol
		if (guardedIdOptimizationOn)
			valueBuilder.append(proofBytes);
	}

	public void loadGuardedValue(Tuple t) {

		this.t = t;
		keyBuilder.setLength(0);
		keyBuilder.append(t.toString());
		
		// add sort indication to key if necessary
		if (round1FiniteMemoryOptimizationOn) {
			keyBuilder.append(proofBytes);
		} 
	}



	/**
	 * Sends out an assert message to this guarded tuple,
	 * to indicate its own existance.
	 * If the guarded ID optimization is on,
	 * the message constant is replaced with a special constant symbol.
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendAssert() throws IOException, InterruptedException {

		if (!guardedIdOptimizationOn) {
			valueBuilder.setLength(0);
			valueBuilder.append(t.toString());
		}
		
		// update counters before sending the message
		ASSERT.increment(1);
		ASSERTBYTES.increment(keyBuilder.length()+valueBuilder.length());
		
		sendMessage();

		
	}

	

	protected void sendMessage() throws IOException, InterruptedException{
		sendMessage(keyBuilder.toString().getBytes(),valueBuilder.toString().getBytes());
	}


	protected void sendMessage(byte[] key, byte[] value) throws IOException, InterruptedException {
		// OPTIMIZE is it necessary to work directly on the Text objects?
		keyText.clear();
		valueText.clear();
		keyText.append(key, 0, key.length);
		valueText.append(value, 0, value.length);
		
		context.write(keyText, valueText);
		
		//System.out.println("<" +keyText.toString()+ " : " + valueText.toString() + ">");
	}

}
