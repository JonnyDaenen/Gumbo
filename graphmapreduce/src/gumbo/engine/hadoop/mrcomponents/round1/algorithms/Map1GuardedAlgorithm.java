package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Map1GuardedAlgorithm {
	
	private static final Log LOG = LogFactory.getLog(Map1GuardedAlgorithm.class);

	Map1GuardedMessageFactory msgFactory;
	ExpressionSetOperations eso;
	

	public Map1GuardedAlgorithm(ExpressionSetOperations eso, Map1GuardedMessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;
	}

	public void run(Tuple t, long offset) throws InterruptedException {
		
		try {
		msgFactory.loadGuardedValue(t);

		// OPTIMIZE search for guarded atom based on relation name
		// guarded ASSERT output
		for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

			// if no guarded expression matches this tuple, it will not be output
			if (guarded.matches(t)) {
				msgFactory.sendAssert();

				// one assert message suffices
				break;
			}
		}
		
		} catch ( Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

}
