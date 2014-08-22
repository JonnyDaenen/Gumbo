/**
 * Created: 21 Aug 2014
 */
package guardedfragment.mapreduce.mappers;

import guardedfragment.mapreduce.planner.structures.data.Tuple;
import guardedfragment.mapreduce.planner.structures.operations.GFMapper;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.io.Pair;
import guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper1Generic extends GFMapper{
	
	private static final Log LOG = LogFactory.getLog(GFMapper1Generic.class);

	Set<GFExistentialExpression> formulaSet;
	
	/**
	 * Set the formula's used.
	 * @param formulaSet the formulaSet to set
	 */
	public void setFormulaSet(Set<GFExistentialExpression> formulaSet) {
		this.formulaSet = formulaSet;
	}

	/**
	 * @see guardedfragment.mapreduce.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Set<Pair<String,String>> map(String value) {
		
		Set<Pair<String,String>> result = new HashSet<Pair<String,String>>();
		
		for (GFExistentialExpression formula : formulaSet) {

			// convert value to tuple
			Tuple t = new Tuple(value.toString());
			LOG.trace("An original value: " + value.toString());

			GFAtomicExpression guard = formula.getGuard();
			Set<GFAtomicExpression> guardedRelations = formula.getChild().getAtomic();

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
