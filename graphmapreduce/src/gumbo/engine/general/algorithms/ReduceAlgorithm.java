package gumbo.engine.general.algorithms;

import gumbo.structures.data.Tuple;

public interface ReduceAlgorithm {
	
	public void initialize(String key) throws AlgorithmInterruptedException;
	public boolean processTuple(String split) throws AlgorithmInterruptedException;
	public void finish() throws AlgorithmInterruptedException;
	public void cleanup() throws AlgorithmInterruptedException;
	
}
