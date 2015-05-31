package gumbo.engine.hadoop.mrcomponents.round1.algorithms;

import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Map1GuardAlgorithm {
	
	private static final Log LOG = LogFactory.getLog(Map1GuardAlgorithm.class);

	Map1GuardMessageFactory msgFactory;
	ExpressionSetOperations eso;
	

	public Map1GuardAlgorithm(ExpressionSetOperations eso, Map1GuardMessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;
	}

	public void run(Tuple t, long offset) throws AlgorithmInterruptedException {

		try {
			msgFactory.loadGuardValue(t,offset);


			boolean outputAssert = false;
			boolean guardIsGuarded = false;

			
			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					// output KAL-R for this guard if necessary
					msgFactory.sendGuardKeepAliveRequest(guard);
					outputAssert = true;

					// projections to atoms
					for (Triple<GFAtomicExpression, GFAtomProjection, Integer> guardedInfo : eso.getGuardedsAndProjections(guard)) {

						GFAtomicExpression guarded = guardedInfo.fst;

						// if guarded is same relation, output special assert afterwards
						if (guarded.getRelationSchema().equals(guard.getRelationSchema())) {
							guardIsGuarded = true;
						}

						msgFactory.sendRequest(guardedInfo);
					}
				} 
			}

			// output an assert message if the guard is also guarded (forced)
			// or if it matched a guard was matched and a Keep-alive assert is required
			if (guardIsGuarded || outputAssert) {
				msgFactory.sendGuardedAssert(guardIsGuarded);
			}
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

}
