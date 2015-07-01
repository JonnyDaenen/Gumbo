package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.reducers.GumboRed1Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Red1MessageFactory {

	private static final Log LOG = LogFactory.getLog(Red1MessageFactory.class);

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
	private Set<Integer> assertKeys;
	private Set<Integer> replyKeys;
	private Set<String> replyKeys2;
	private boolean outGroupingOn;
	private boolean reqAtomIdOn;
	private ExpressionSetOperations eso;

	public Red1MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso, String filename) {
		keyText = new Text();
		valueText = new Text();

		// ---
		OUTR = context.getCounter(GumboRed1Counter.RED1_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed1Counter.RED1_OUT_BYTES);

		ABORTS = context.getCounter(GumboRed1Counter.RED1_PREMATURE_ABORTS);
		BUFFERED = context.getCounter(GumboRed1Counter.RED1_BUFFEREDITEMS);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);

		outGroupingOn = settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn);
		reqAtomIdOn = settings.getBooleanProperty(AbstractExecutorSettings.requestAtomIdOptimizationOn);


		mos = new MultipleOutputs<>(context);
		this.filename = filename;
		this.eso = eso;

		assertKeys = null;
		replyKeys = new HashSet<>(10);



	}

	public void loadValue(String address, String reply)  {
		keyText.clear();
		valueText.clear();

		keyText.set(address);
		valueText.set(reply);

		if (outGroupingOn) {
			replyKeys.clear();
			String [] parts = reply.split(",");
			// start at second index to skip Assert constant/value
			for (int i = 1; i < parts.length; i++) {
				if (reqAtomIdOn)
					replyKeys.add(Integer.parseInt(parts[i]));
				else {


					Tuple atomTuple = new Tuple(parts[i]);
					GFAtomicExpression dummy = new GFAtomicExpression(atomTuple.getName(), atomTuple.getAllData());
					try {
						replyKeys.add(eso.getAtomId(dummy));
					} catch (GFOperationInitException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

	}

	public void sendReplies() throws MessageFailedException {

		// only send out replies that have an answer

		if (outGroupingOn) { 

			replyKeys.retainAll(assertKeys);

			for (int replyid : replyKeys) {
				valueText.clear();
				if (reqAtomIdOn) {
					valueText.set(""+replyid);
				} else {
					try {
						valueText.set(eso.getAtom(replyid).toString());
					} catch (GFOperationInitException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}

				OUTB.increment(keyText.getLength()+valueText.getLength());
				sendMessage(); // OPTIMIZE bundle all these values for round 2

			}
			OUTR.increment(replyKeys.size());
		} else {
//			LOG.info("Out: " + keyText + " : " + valueText);
			OUTR.increment(1);
			OUTB.increment(keyText.getLength()+valueText.getLength());

			sendMessage();
		}

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

	public void setKeys(Set<Integer> keysFound) {
		this.assertKeys = keysFound;

	}




}
