package gumbo.engine.general.messagefactories;

import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

import java.io.IOException;
import java.util.Set;

public interface Map1GuardMessageFactoryInterface {

	public void enableSampleCounting();

	public void loadGuardValue(Tuple t, long offset) throws TupleIDError;

	/**
	 * Sends a Guard keep-alive request message.
	 * The key of the message is the tuple itself,
	 * in string representation. The value
	 * consists of a reply address and a reply value.
	 * The reply address is a reference to the tuple,
	 * the reply value is a reference to a guarded atom.
	 * Both representations can be an id, or the object 
	 * in string representation, depending on the 
	 * optimization settings.
	 * 
	 * @param guard
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws GFOperationInitException
	 */
	public void sendGuardKeepAliveRequest(GFAtomicExpression guard)
			throws MessageFailedException;

	/**
	 * Sends out an assert message to the guard tuple,
	 * as part of the Keep-alive system.
	 * The key is the tuple itself, in string representation.
	 * The value consists of a reference to the tuple,
	 * which can either be the tuple itself, or an id
	 * representing the tuple. This is dependent of the
	 * optimization settings.
	 * 
	 * When finite memory optimization is enabled,
	 * the key is padded with a special symbol in order
	 * to sort, partition and group correctly.
	 * 
	 * <b>Important:</b> when keep-alive optimization is enabled, 
	 * no message will be sent, unless force is true.
	 * 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void sendGuardedAssert(boolean force, Set<Integer> ids)
			throws MessageFailedException;

	public void sendRequest(
			Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo)
			throws MessageFailedException;

	public void finish() throws MessageFailedException;

}