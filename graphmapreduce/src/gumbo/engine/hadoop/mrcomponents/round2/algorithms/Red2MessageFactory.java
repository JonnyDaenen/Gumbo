package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.messagefactories.Red2MessageFactoryInterface;
import gumbo.engine.hadoop.mrcomponents.round2.reducers.GumboRed2Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Red2MessageFactory implements Red2MessageFactoryInterface {


	private static final Log LOG = LogFactory.getLog(Red2MessageFactory.class);
	
	Text keyText;
	Text valueText;


	protected MultipleOutputs<Text, Text> mos;

	private Counter OUTR;
	private Counter OUTB;
	private Counter EXCEPT;
	private Counter TUPLES;
	private Counter TRUE;
	private Counter FALSE;



	// components
	private ExpressionSetOperations eso;

	// data
	Tuple t;
	String tRef;
	String proofBytes;
	String filename;

	public Red2MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.eso = eso;

		// ---
		// counters
		EXCEPT = context.getCounter(GumboRed2Counter.RED2_TUPLE_EXCEPTIONS);
		TUPLES = context.getCounter(GumboRed2Counter.RED2_TUPLES_FOUND);
		TRUE = context.getCounter(GumboRed2Counter.RED2_EVAL_TRUE);
		FALSE = context.getCounter(GumboRed2Counter.RED2_EVAL_FALSE);


		OUTR = context.getCounter(GumboRed2Counter.RED2_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed2Counter.RED2_OUT_BYTES);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


		mos = new MultipleOutputs<>(context);


	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#loadValue(gumbo.structures.data.Tuple)
	 */
	@Override
	public void loadValue(Tuple t) {
		this.filename = generateFileName(t);
		valueText.clear();
		valueText.set(t.toString());
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#sendOutput()
	 */
	@Override
	public void sendOutput() throws MessageFailedException {
		OUTR.increment(1);
		OUTB.increment(valueText.getLength());
		sendMessage();
	}



	protected void sendMessage() throws MessageFailedException{
		try {

			mos.write((Text)null, valueText, filename);
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}


	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#cleanup()
	 */
	@Override
	public void cleanup() throws MessageFailedException {
		try {
			mos.close();
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}


	protected String generateFileName(Tuple t) {
		RelationSchema rs = new RelationSchema(t.getName(),t.size());
		// OPTIMIZE add cache
		Set<Path> paths = eso.getFileMapping().getPaths(rs);
		for (Path path: paths) {
			return path.toString() + "/" + rs.getName();
		}
		return ""; // FIXME fallback system + duplicate code in other reducer2

	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#getTuple(java.lang.String)
	 */
	@Override
	public Tuple getTuple(String value) {
		// OPTIMIZE this may be slow
		return new Tuple(value.substring(1));
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#isTuple(java.lang.String)
	 */
	@Override
	public boolean isTuple(String value) {
		return value.startsWith(proofBytes);
	}


	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#incrementExcept(long)
	 */
	@Override
	public void incrementExcept(long incr) {
		EXCEPT.increment(incr);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#incrementFalse(long)
	 */
	@Override
	public void incrementFalse(long incr) {
		FALSE.increment(incr);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#incrementTrue(long)
	 */
	@Override
	public void incrementTrue(long incr) {
		TRUE.increment(incr);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round2.algorithms.Red2MessageFactoryInterface#incrementTuples(long)
	 */
	@Override
	public void incrementTuples(long incr) {
		TUPLES.increment(incr);
	}


}
