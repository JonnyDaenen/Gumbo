package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

public class AlgorithmInterruptedException extends Exception {

	public AlgorithmInterruptedException(Exception e) {
		super(e);
	}

	public AlgorithmInterruptedException(String s) {
		super(s);
	}
}
