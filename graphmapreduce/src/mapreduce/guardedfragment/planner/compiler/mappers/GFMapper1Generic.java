/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper1Generic extends GFMapper implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(GFMapper1Generic.class);

	
	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Set<Pair<String,String>> map(String value, Collection<GFExistentialExpression> expressionSet) {
		
		Set<Pair<String,String>> result = new HashSet<Pair<String,String>>();
		
		for (GFExistentialExpression formula : expressionSet) {

			// convert value to tuple
			Tuple t = new Tuple(value.toString());
			LOG.trace("An original value: " + value.toString());

			GFAtomicExpression guard = formula.getGuard();
			// OPTIMIZE pre-calculate
			Collection<GFAtomicExpression> guardedRelations = formula.getChild().getAtomic();

			// check if tuple matches guard
			if (guard.matches(t)) {

				// output guard:guard
				//context.write(new Text(t.toString()), new Text(t.toString()));
				addOutput(t.toString(), t.toString(), result);
				
				LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());

				for (GFAtomicExpression guarded : guardedRelations) {

					// get projection
					// OPTIMIZE do this when initializing
					GFAtomProjection gp = new GFAtomProjection(guard, guarded);
					Tuple tprime;
					try {
						// project to key
						tprime = gp.project(t);

						if (guarded.matches(tprime)) {

							// LOG.error(tprime + " matches' " + guarded +
							// " val: " + t);
							// project to key and write out
							//context.write(new Text(tprime.toString()), new Text(t.toString()));
							addOutput(tprime.toString(), t.toString(), result);
							LOG.trace("The first Mapper outputs the pair: " + tprime.toString() + " : " + t.toString());

						}
					} catch (NonMatchingTupleException e1) {
						// should not happen!
						e1.printStackTrace();
					}

				}

			}

			
			// ATTENTION: do not use ELSE here
			// the guard can appear as guarded too!
			
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// if so, output tuple with same key and value
				if (guarded.matches(t)) {
					// LOG.error(t + " matches " + guarded);
					//context.write(new Text(t.toString()), new Text(t.toString()));
					addOutput(t.toString(), t.toString(), result);
					LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());
					// break;
				}
			}

		}
		
		return result;
		
	}

}
