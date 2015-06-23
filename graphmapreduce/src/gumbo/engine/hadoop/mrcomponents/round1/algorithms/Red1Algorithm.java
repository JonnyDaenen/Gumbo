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

	private boolean finiteMemOptOn;

	Set<Pair<String, String>> buffer;


	public Red1Algorithm(ExpressionSetOperations eso, AbstractExecutorSettings settings, Red1MessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;

		// --- opts
		finiteMemOptOn = settings.getBooleanProperty(AbstractExecutorSettings.round1FiniteMemoryOptimizationOn);

		// counters

	}

	public void initialize(String key) throws AlgorithmInterruptedException{
		keyFound = false;
		buffer = new HashSet<>(10);
		
//		if (key.contains("S(")){
//			int i = 0;
//		}
	}

	/**
	 * 
	 * @param split
	 * @return false when all next values of this key can be skipped
	 * @throws AlgorithmInterruptedException
	 */
	public boolean processTuple(Pair<String,String> split) throws AlgorithmInterruptedException {

		try {
			// is this not the key (key is only thing that can appear without atom reference)
			// it does not matter whether it's sent as S(1) or with a constant symbol such as '#'
			if (split.snd.length() > 0) {

				msgFactory.loadValue(split.fst, split.snd);

				// if the key has already been found, we can output
				if (keyFound) {
					msgFactory.sendReply();
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
				keyFound = true;
			}
			
			return true;
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}

	}

	public void finish() throws AlgorithmInterruptedException {

		try {
			// output the remaining data
			if (keyFound) {
				for (Pair<String, String> p : buffer) {
					msgFactory.loadValue(p.fst, p.snd);
					msgFactory.sendReply();
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
