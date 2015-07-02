package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.structures.data.Tuple;

public interface MapAlgorithm {
	public void run(Tuple t, long offset) throws AlgorithmInterruptedException;
}
