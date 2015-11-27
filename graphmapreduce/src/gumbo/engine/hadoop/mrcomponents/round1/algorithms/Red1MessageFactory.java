package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.messagefactories.Red1MessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.reducers.GumboRed1Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
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

public class Red1MessageFactory implements Red1MessageFactoryInterface {

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
	private Set<Integer> requestKeys;
	private boolean outMapGroupingOn;
	private boolean outRedGroupingOn;
	private boolean reqAtomIdOn;
	private ExpressionSetOperations eso;

	StringBuilder sb;


	public Red1MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso, String filename) {
		keyText = new Text();
		valueText = new Text();

		// ---
		OUTR = context.getCounter(GumboRed1Counter.RED1_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed1Counter.RED1_OUT_BYTES);

		ABORTS = context.getCounter(GumboRed1Counter.RED1_PREMATURE_ABORTS);
		BUFFERED = context.getCounter(GumboRed1Counter.RED1_BUFFEREDITEMS);


		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);

		outMapGroupingOn = settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn);
		outRedGroupingOn = settings.getBooleanProperty(AbstractExecutorSettings.reduceOutputGroupingOptimizationOn);
		reqAtomIdOn = settings.getBooleanProperty(AbstractExecutorSettings.requestAtomIdOptimizationOn);


		mos = new MultipleOutputs<>(context);
		this.filename = filename;
		this.eso = eso;

		assertKeys = null;
		requestKeys = new HashSet<>(10);


		sb = new StringBuilder(40);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#loadValue(java.lang.String, java.lang.String)
	 */
	@Override
	public void loadValue(String address, String reply)  {
		
		keyText.clear();
		valueText.clear();

		keyText.set(address);
		valueText.set(reply);

		if (outMapGroupingOn) {
			requestKeys.clear();
			String [] parts = reply.split(":");
			// start at second index to skip Assert constant/value
			for (int i = 0; i < parts.length; i++) {
				if (reqAtomIdOn)
					requestKeys.add(Integer.parseInt(parts[i]));
				else {


					Tuple atomTuple = new Tuple(parts[i]);
					GFAtomicExpression dummy = new GFAtomicExpression(atomTuple.getName(), atomTuple.getAllData());
					try {
						requestKeys.add(eso.getAtomId(dummy));
					} catch (GFOperationInitException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#sendReplies()
	 */
	@Override
	public void sendReplies() throws MessageFailedException {

		try {
			// only send out replies that have an answer

			if (outMapGroupingOn) { 
				requestKeys.retainAll(assertKeys);
				
				if (requestKeys.size() == 0)
					return;

				if (outRedGroupingOn) { // TODO this can be separate from the map output grouping

					valueText.clear();
					sb.setLength(0);
					
					for (int replyid : requestKeys) {
						sb.append(":");
						if (reqAtomIdOn) {
							sb.append(replyid);
						} else {
							sb.append(eso.getAtom(replyid).toString());
						}
					}
					byte [] bytes = sb.substring(1).getBytes();
					valueText.append(bytes, 0, bytes.length);

					OUTB.increment(keyText.getLength()+valueText.getLength());
					sendMessage();
					OUTR.increment(1);
					
				} else {

					for (int replyid : requestKeys) {
						valueText.clear();

						if (reqAtomIdOn) {
							valueText.set(""+replyid);
						} else {
							valueText.set(eso.getAtom(replyid).toString());
						}

						OUTB.increment(keyText.getLength()+valueText.getLength());
						sendMessage(); 
						OUTR.increment(1);

					}
				}



			} else {
				//			LOG.info("Out: " + keyText + " : " + valueText);
				OUTR.increment(1);
				OUTB.increment(keyText.getLength()+valueText.getLength());

				sendMessage();
			}

		} catch (GFOperationInitException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}


	protected void sendMessage() throws MessageFailedException{
		try {

			//			LOG.info("Reply: " + keyText + " : " + valueText);

//			LOG.error(keyText + ": " + valueText);
			mos.write(keyText, valueText, filename);
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}


	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#cleanup()
	 */
	@Override
	public void cleanup() throws MessageFailedException {
		try {
			mos.close();
		} catch(Exception e) {
			throw new MessageFailedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#addAbort(long)
	 */
	@Override
	public void addAbort(long incr) {
		ABORTS.increment(incr);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#addBuffered(long)
	 */
	@Override
	public void addBuffered(long incr) {
		BUFFERED.increment(incr);
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.hadoop.mrcomponents.round1.algorithms.Red1MessageFactoryInterface#setKeys(java.util.Set)
	 */
	@Override
	public void setKeys(Set<Integer> keysFound) {
		this.assertKeys = keysFound;

	}




}
