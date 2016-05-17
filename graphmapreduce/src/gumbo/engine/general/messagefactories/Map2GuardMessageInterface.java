package gumbo.engine.general.messagefactories;

import java.io.IOException;

import gumbo.structures.data.Tuple;

public interface Map2GuardMessageInterface {

	public void loadGuardValue(Tuple t, long offset)
			throws MessageFailedException;

	/**
	 * Sends an assert message to
	 * the guard reference,
	 * containing a special assert
	 * message. This latter message
	 * is only useful when using pointers
	 * for the guard tuples and is needed 
	 * to restore the original guard tuple.
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendGuardAssert() throws MessageFailedException;

}