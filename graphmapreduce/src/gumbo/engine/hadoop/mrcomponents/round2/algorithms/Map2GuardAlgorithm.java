package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.AlgorithmInterruptedException;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Map2GuardAlgorithm {

	private static final Log LOG = LogFactory.getLog(Map2GuardAlgorithm.class);

	Map2GuardMessageFactory msgFactory;
	ExpressionSetOperations eso;


	public Map2GuardAlgorithm(ExpressionSetOperations eso, Map2GuardMessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;
	}

	public void run(Tuple t, long offset) throws AlgorithmInterruptedException {
		try { 
			msgFactory.loadGuardValue(t,offset);

			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {
				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {
					msgFactory.sendGuardAssert();
					// one assert is enough
					break;
				}			
			}

		} catch ( Exception e) {
			throw new AlgorithmInterruptedException(e);
		} 

	}

}
