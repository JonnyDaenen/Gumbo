package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.reducers.GumboRed1Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Red1MessageFactory {


	Text keyText;
	Text valueText;


	protected MultipleOutputs<Text, Text> mos;


	private Counter OUTR;
	private Counter OUTB;
	private Counter ABORTS;
	private Counter BUFFERED;


	// components

	// data
	Tuple t;
	String tRef;
	String proofBytes;
	String filename;

	public Red1MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso, String filename) {
		keyText = new Text();
		valueText = new Text();

		// ---
		OUTR = context.getCounter(GumboRed1Counter.RED1_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed1Counter.RED1_OUT_BYTES);

		ABORTS = context.getCounter(GumboRed1Counter.RED1_PREMATURE_ABORTS);
		BUFFERED = context.getCounter(GumboRed1Counter.RED1_BUFFEREDITEMS);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);


		mos = new MultipleOutputs<>(context);
		this.filename = filename;


	}

	public void loadValue(String address, String reply)  {
		keyText.clear();
		valueText.clear();

		keyText.set(address);
		valueText.set(reply);

	}

	public void sendReply() throws MessageFailedException {
		OUTR.increment(1);
		OUTB.increment(keyText.getLength()+valueText.getLength());
		sendMessage();

	}


	protected void sendMessage() throws MessageFailedException{
		try {
			mos.write(keyText, valueText, filename);
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

	public void addAbort(long incr) {
		ABORTS.increment(incr);
	}

	public void addBuffered(long incr) {
		BUFFERED.increment(incr);
	}




}