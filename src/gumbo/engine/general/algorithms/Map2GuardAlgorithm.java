package gumbo.engine.general.algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.engine.general.messagefactories.Map2GuardMessageInterface;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

public class Map2GuardAlgorithm implements MapAlgorithm {

	private static final Log LOG = LogFactory.getLog(Map2GuardAlgorithm.class);

	Map2GuardMessageInterface msgFactory;
	ExpressionSetOperations eso;


	public Map2GuardAlgorithm(ExpressionSetOperations eso, Map2GuardMessageInterface msgFactory) {
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
