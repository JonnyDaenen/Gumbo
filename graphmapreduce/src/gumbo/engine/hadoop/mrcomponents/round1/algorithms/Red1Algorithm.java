package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Red1Algorithm {

	private static final Log LOG = LogFactory.getLog(Red1Algorithm.class);

	Red1MessageFactory msgFactory;
	ExpressionSetOperations eso;

	boolean keyFound;
	Set<Integer> keysFound;

	private boolean finiteMemOptOn;
	private boolean outGroupingOn;

	Set<Pair<String, String>> buffer;



	public Red1Algorithm(ExpressionSetOperations eso, AbstractExecutorSettings settings, Red1MessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;

		// --- opts
		finiteMemOptOn = settings.getBooleanProperty(AbstractExecutorSettings.round1FiniteMemoryOptimizationOn);
		outGroupingOn = settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn);

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
				if (outGroupingOn && finiteMemOptOn) {
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
				if (outGroupingOn) {
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
			if (keyFound || outGroupingOn) {
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

	public void cleanup() throws MessageFailedException {
		msgFactory.cleanup();

	}

}
