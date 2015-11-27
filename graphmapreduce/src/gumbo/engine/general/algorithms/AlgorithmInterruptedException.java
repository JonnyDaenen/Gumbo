package gumbo.engine.general.algorithms;

public class AlgorithmInterruptedException extends Exception {

	public AlgorithmInterruptedException(Exception e) {
		super(e);
	}

	public AlgorithmInterruptedException(String s) {
		super(s);
	}
}
