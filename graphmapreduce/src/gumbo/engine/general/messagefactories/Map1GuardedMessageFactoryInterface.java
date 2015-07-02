package gumbo.engine.general.messagefactories;

import gumbo.structures.data.Tuple;

import java.io.IOException;
import java.util.Set;

public interface Map1GuardedMessageFactoryInterface {

	public void enableSampleCounting();

	public void loadGuardedValue(Tuple t);

	public void sendAssert() throws MessageFailedException;

	/**
	 * Sends out an assert message to this guarded tuple,
	 * to indicate its own existance.
	 * 
	 * If the guarded ID optimization is on,
	 * the message constant is replaced with a special constant symbol.
	 * 
	 * If map output grouping is on, the atom ids of matching
	 * atoms are also sent as a list. Note that, if the supplied list is empty,
	 * this methods acts as if grouping is off and just sends the 
	 * message. This means that checking if the message should be sent
	 * should be done before calling this method.
	 * 
	 * @param ids the list of atom ids that match the tuple
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendAssert(Set<Integer> ids) throws MessageFailedException;

}