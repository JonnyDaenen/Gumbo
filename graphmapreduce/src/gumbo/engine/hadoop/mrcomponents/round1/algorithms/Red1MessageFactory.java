package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import gumbo.engine.hadoop.mrcomponents.round1.reducers.GumboRed1Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

public class Red1MessageFactory {


	Text keyText;
	Text valueText;
	

	protected MultipleOutputs<Text, Text> mos;

	private boolean guardTuplePointerOptimizationOn;
	private boolean guardKeepaliveOptimizationOn;
	private boolean round1FiniteMemoryOptimizationOn;
	private boolean guardIdOptimizationOn;
	private boolean guardedIdOptimizationOn;

	private Counter OUTR;
	private Counter OUTB;
	
	private Reducer<Text, Text, Text, Text>.Context context;

	// components
	private ExpressionSetOperations eso;
	private StringBuilder keyBuilder;
	private StringBuilder valueBuilder;

	// data
	Tuple t;
	String tRef;
	String proofBytes;
	String filename;

	public Red1MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso, String filename) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		this.eso = eso;
		keyBuilder = new StringBuilder(16);
		valueBuilder = new StringBuilder(128);

		// ---
		guardTuplePointerOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveReductionOn);
		guardKeepaliveOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.guardKeepAliveReductionOn);
		round1FiniteMemoryOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.round1FiniteMemoryOptimizationOn);
		guardIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.requestAtomIdOptimizationOn);
		guardedIdOptimizationOn = settings.getBooleanProperty(HadoopExecutorSettings.assertConstantOptimizationOn);


		// ---
		OUTR = context.getCounter(GumboRed1Counter.RED1_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed1Counter.RED1_OUT_BYTES);
		

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

	public void sendReply() throws InterruptedException, IOException {
		OUTR.increment(1);
		OUTB.increment(keyText.getLength()+valueText.getLength());
		sendMessage();
		
	}


	protected void sendMessage() throws InterruptedException, IOException{
		mos.write(keyText, valueText, filename);
	}


	public void cleanup() throws IOException, InterruptedException {
		mos.close();
	}





}
