package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

public class Map2GuardAlgorithm {
	
	private static final Log LOG = LogFactory.getLog(Map2GuardAlgorithm.class);

	Map2GuardMessageFactory msgFactory;
	ExpressionSetOperations eso;
	

	public Map2GuardAlgorithm(ExpressionSetOperations eso, Map2GuardMessageFactory msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;
	}

	public void run(Tuple t, long offset) throws InterruptedException {
		try { 
			msgFactory.loadGuardValue(t,offset);
			
			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {
				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {
					msgFactory.sendGuardAssert();
				}			
			}

		} catch ( Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

}
