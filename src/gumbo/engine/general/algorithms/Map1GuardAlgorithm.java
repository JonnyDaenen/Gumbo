package gumbo.engine.general.algorithms;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.engine.general.messagefactories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

public class Map1GuardAlgorithm implements MapAlgorithm{
	
	private static final Log LOG = LogFactory.getLog(Map1GuardAlgorithm.class);

	Map1GuardMessageFactoryInterface msgFactory;
	ExpressionSetOperations eso;

	private boolean keepAliveOn;
	

	public Map1GuardAlgorithm(ExpressionSetOperations eso, Map1GuardMessageFactoryInterface msgFactory, AbstractExecutorSettings settings) {
		this.msgFactory = msgFactory;
		this.eso = eso;
		keepAliveOn = !settings.getBooleanProperty(AbstractExecutorSettings.guardKeepAliveOptimizationOn);
		
	}

	public void run(Tuple t, long offset) throws AlgorithmInterruptedException {

		try {
			msgFactory.loadGuardValue(t,offset); // do not postpone this, for counting purposes


			boolean outputAssert = false;
			boolean guardIsGuarded = false;
			
			Set<Integer> ids = new HashSet<>();

			
			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {
				
				// add extra id to make sure keep alive is picked up in grouping mode
				// when grouping mode is off, this is transmitted, but ignored by the reducer.
				if (keepAliveOn) {
					ids.add(eso.getAtomId(guard));
				}

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
							ids.add(guardedInfo.trd);
						}

						msgFactory.sendRequest(guardedInfo);
					}
				} 
			}

			// output an assert message if the guard is also guarded (forced)
			// or if it matched a guard was matched and a Keep-alive assert is required
			if (guardIsGuarded || outputAssert) {
				msgFactory.sendGuardedAssert(guardIsGuarded,ids); // TODO apply grouping here
			}
			
			msgFactory.finish();
			
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

}
