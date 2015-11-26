package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import gumbo.engine.general.messagefactories.Map2GuardMessageInterface;
import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.hadoop.mrcomponents.round2.mappers.GumboMap2Counter;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2GuardMessageFactory implements Map2GuardMessageInterface {

	Text keyText;
	Text valueText;
	private Counter ASSERT;
	private Counter ASSERTBYTES;

	private boolean guardTuplePointerOptimizationOn;
	private Mapper<LongWritable, Text, Text, Text>.Context context;

	// components
	private TupleIDCreator pathids;

	// data
	Tuple t;
	String tRef;
	String proofBytes;

	public Map2GuardMessageFactory(Mapper<LongWritable, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		this.pathids = new TupleIDCreator(eso.getFileMapping());

		// ---
		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardReferenceOptimizationOn);

		// ---
		ASSERT = context.getCounter(GumboMap2Counter.ASSERT_RECORDS);
		ASSERTBYTES = context.getCounter(GumboMap2Counter.ASSERT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Map2GuardMessageInterface#loadGuardValue(gumbo.structures.data.Tuple, long)
	 */
	@Override
	public void loadGuardValue(Tuple t, long offset) throws MessageFailedException {

		try {
			this.t = t;
			keyText.clear();
			valueText.clear();

			byte[] bytes = t.toString().getBytes();

			// replace value with pointer when optimization is on
			if (guardTuplePointerOptimizationOn) {
				keyText.set(pathids.getTupleID(context, offset)); // key indicates offset in TextInputFormat
			} else
				keyText.set(bytes, 0, bytes.length);

			valueText.set(proofBytes);
			valueText.append(bytes, 0, bytes.length);
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}

	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Map2GuardMessageInterface#sendGuardAssert()
	 */
	@Override
	public void sendGuardAssert() throws MessageFailedException {

		ASSERT.increment(1);
		ASSERTBYTES.increment(keyText.getLength() + valueText.getLength());
		sendMessage();


	}


	protected void sendMessage() throws MessageFailedException{
		try {
			context.write(keyText, valueText);
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}




}
