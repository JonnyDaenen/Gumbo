package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.MessageFailedException;
import gumbo.engine.hadoop.mrcomponents.round2.reducers.GumboRed2Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Red2MessageFactory {


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

	public void loadValue(Tuple t) {
		this.filename = generateFileName(t);
		valueText.clear();
		valueText.set(t.toString());
	}

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

	public Tuple getTuple(String value) {
		// OPTIMIZE this may be slow
		return new Tuple(value.substring(1));
	}

	public boolean isTuple(String value) {
		return value.startsWith(proofBytes);
	}


	public void incrementExcept(long incr) {
		EXCEPT.increment(incr);
	}

	public void incrementFalse(long incr) {
		FALSE.increment(incr);
	}

	public void incrementTrue(long incr) {
		TRUE.increment(incr);
	}

	public void incrementTuples(long incr) {
		TUPLES.increment(incr);
	}


}
