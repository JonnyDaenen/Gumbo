/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

import gumbo.engine.hadoop.mrcomponents.mappers.TupleIDCreator.TupleIDError;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.io.Triple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;
import gumbo.structures.gfexpressions.operations.NonMatchingTupleException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardRelOptimized extends GFMapper1Identity {



	private static final Log LOG = LogFactory.getLog(GFMapper1GuardRelOptimized.class);

	protected Map1GuardMessageFactory msgFactory;



	/**
	 * @see gumbo.engine.hadoop.mrcomponents.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		msgFactory = new Map1GuardMessageFactory(context,settings,eso);		
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try {

			// transform data
			Tuple t = new Tuple(value.getBytes());
			msgFactory.loadGuardValue(t,key.get());
				
			
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
			// or if it matched a guard was matched
			if (guardIsGuarded || outputAssert) {
				msgFactory.sendGuardedAssert(guardIsGuarded);
			}



		} catch ( TupleIDError | NonMatchingTupleException | GFOperationInitException e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}




}
