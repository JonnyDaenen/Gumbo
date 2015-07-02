package gumbo.engine.general.algorithms;

import gumbo.engine.general.factories.Map1GuardedMessageFactoryInterface;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Map1GuardedAlgorithm implements MapAlgorithm {

	private static final Log LOG = LogFactory.getLog(Map1GuardedAlgorithm.class);

	Map1GuardedMessageFactoryInterface msgFactory;
	ExpressionSetOperations eso;
	boolean sendIds;

	Set<Integer> ids;

	/**
	 * 
	 * @param eso
	 * @param msgFactory
	 * @param sendIds toggle to enable sending the matching atom ids as part of the assert message
	 */
	public Map1GuardedAlgorithm(ExpressionSetOperations eso, Map1GuardedMessageFactoryInterface msgFactory, boolean sendIds) {
		this.msgFactory = msgFactory;
		this.eso = eso;
		this.sendIds = sendIds;
		
		ids = new HashSet<>();
	}

	public void run(Tuple t, long offset) throws AlgorithmInterruptedException {

		try {
			msgFactory.loadGuardedValue(t); // do not postpone this, for counting purposes

			boolean output = false;
			ids.clear();

			// OPTIMIZE search for guarded atom based on relation name
			// guarded ASSERT output
			for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

				// if no guarded expression matches this tuple, it will not be output
				if (guarded.matches(t)) {

					output = true;

					// buffer the ids if necessary
					if (sendIds) {
						ids.add(eso.getAtomId(guarded));
					} else {
						break;
					}
				}
			}

			// send the assert message
			if (output) {
				msgFactory.sendAssert(ids);
			}

		} catch ( Exception e) {
			throw new AlgorithmInterruptedException(e);
		} 

	}

}
