package gumbo.engine.general.algorithms;

import gumbo.engine.general.messagefactories.MessageFailedException;
import gumbo.engine.general.messagefactories.Red1MessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Red1Algorithm implements ReduceAlgorithm {

	private static final Log LOG = LogFactory.getLog(Red1Algorithm.class);

	Red1MessageFactoryInterface msgFactory;
	ExpressionSetOperations eso;

	boolean keyFound;
	Set<Integer> keysFound;

	private boolean finiteMemOptOn;
	private boolean mapOutGroupingOn;

	Set<Pair<String, String>> buffer;



	public Red1Algorithm(ExpressionSetOperations eso, AbstractExecutorSettings settings, Red1MessageFactoryInterface msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;

		// --- opts
		finiteMemOptOn = settings.getBooleanProperty(AbstractExecutorSettings.round1FiniteMemoryOptimizationOn);
		mapOutGroupingOn = settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn);

		// counters

		// keys
		keysFound = new HashSet<>(10);
		

	}

	public void initialize(String key) throws AlgorithmInterruptedException{
		keyFound = false;
		buffer = new HashSet<>(10); // we do this here, in case the buffer allocation was too large in prev round
		keysFound.clear();
		msgFactory.setKeys(keysFound);

		//		if (key.contains("S(")){
		//			int i = 0;
		//		}
	}

	/**
	 * 
	 * @param split
	 * 
	 * @return false when all next values of this key can be skipped
	 * 
	 * @throws AlgorithmInterruptedException
	 */
	public boolean processTuple(String value) throws AlgorithmInterruptedException {
		Pair<String,String> split = split(value.getBytes(), value.length());
		return processTuple(split);
	}
	
	public boolean processTuple(Pair<String,String> split) throws AlgorithmInterruptedException {	
		
		try {
			// is this not the key (key is only thing that can appear without atom reference)
			// it does not matter whether it's sent as S(1) or with a constant symbol such as '#'
			if (split.snd.length() > 0) {

				msgFactory.loadValue(split.fst, split.snd);
				
				
				// if both options are on
				// we know that all necessary keys have been collected
				// so we can start outputting
				// note that the factory has all the keys (see init)
				if (mapOutGroupingOn && finiteMemOptOn) {
					keyFound = true;
				}

				// if the key has already been found, we can output
				if (keyFound) {
					msgFactory.sendReplies();
				}
				// if optimization is on, we know that if the key is not there, we can skip the rest
				else if (finiteMemOptOn) {
					msgFactory.addAbort(1);
					return false;
				} 
				// otherwise, we buffer the data
				else {
					buffer.add(split);
					msgFactory.addBuffered(1);

				}

			} // if this is the key, we mark it
			else if (!keyFound) {

				// if grouping is on
				// key looks like #,id1,id2,id3,...,idn

				// collect all ids
				if (mapOutGroupingOn) {
					collectIds(split.fst);
					
				// if there is no grouping, there is only one possible key
				} else {
					keyFound = true;
				}

			}

			return true;
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}

	}

	private void collectIds(String s) {
		String [] parts = s.split(":");
		// start at second index to skip Assert constant/value
		for (int i = 1; i < parts.length; i++) {
			keysFound.add(Integer.parseInt(parts[i]));
		}
		
	}

	public void finish() throws AlgorithmInterruptedException {
		
		try {
			// output the remaining data
			// when grouping is active we start outputting anyway,
			// as every "key" has been parsed
			if (keyFound || mapOutGroupingOn) {
				for (Pair<String, String> p : buffer) {
					msgFactory.loadValue(p.fst, p.snd);
					msgFactory.sendReplies();
				}
			}

			// clear the buffer for next round
			// this is done in the end in case hadoop invokes GC
			// (not sure whether hadoop does this in between calls)
			buffer.clear();
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

	public void cleanup() throws AlgorithmInterruptedException {
		
		try {
			msgFactory.cleanup();
		} catch (MessageFailedException e) {
			throw new AlgorithmInterruptedException(e);
		}

	}
	
	/**
	 * Splits String into 2 parts. String is supposed to be separated with ';'.
	 * When no ';' is present, the numeric value is -1. 
	 * @param t
	 */
	public Pair<String, String> split(byte []  bytes, int length) {


		int pos = -1;
		for (int i = 0; i < length; i++) {
			if (bytes[i] == ';') {
				pos = i;
				break;
			}
		}

		String address = "";
		String reply = "";

		if (pos != -1) {
			address = new String(bytes, 0, pos);
			reply = new String(bytes, pos+1,length-pos-1); // 1-offset is to skip ';'
		} else {
			address = new String(bytes,0,length);
			reply = "";
		}

		return new Pair<>(address, reply);

	}

}
