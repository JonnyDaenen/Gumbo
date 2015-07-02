package gumbo.engine.general.factories;

import gumbo.structures.data.Tuple;

public interface Red2MessageFactoryInterface {

	public void loadValue(Tuple t);

	public void sendOutput() throws MessageFailedException;

	public void cleanup() throws MessageFailedException;

	public Tuple getTuple(String value);

	public boolean isTuple(String value);

	public void incrementExcept(long incr);

	public void incrementFalse(long incr);

	public void incrementTrue(long incr);

	public void incrementTuples(long incr);

}