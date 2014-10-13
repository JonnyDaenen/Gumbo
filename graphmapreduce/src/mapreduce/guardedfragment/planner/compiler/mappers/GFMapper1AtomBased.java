/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.io.IOException;
import java.io.Serializable;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1AtomBased extends GFMapper implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(GFMapper1AtomBased.class);


	/**
	 * @throws GFOperationInitException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Iterable<Pair<Text, Text>> map(Text value) throws GFOperationInitException {
		return new GFMapper1AtomBasedIterator(getGuardsAll(), getGuardedsAll(), getGGPairsAll(), value);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws GFOperationInitException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException, GFOperationInitException {

		Tuple t = new Tuple(value);
		// System.out.println(t);
		

		Text out1 = new Text();
		Text out2 = new Text();

		boolean outputGuard = false;

		// check guards + atom (keep-alive)
		for (GFAtomicExpression guard : getGuardsAll()) {
			if (guard.matches(t)) {

				int guardID = getAtomId(guard);
				
				out1.set(t.toString() + ";" + guardID);
				context.write(value, out1);
//				LOG.warn(value.toString() + " " + out1.toString());
				outputGuard = true;

				// projections to atoms
				for (GFAtomicExpression guarded : getGuardeds(guard)) {
					try {
						GFAtomProjection p = getProjections(guard, guarded);
						Tuple tprime = p.project(t);

						// TODO why is this check necessary?
						if (guarded.matches(tprime)) {
							out1.set(tprime.toString());
							int guardedID = getAtomId(guarded);
							out2.set(t.toString() + ";" + guardedID);
							context.write(out1, out2);
//							LOG.warn(out1.toString() + " " + out2.toString());
//							LOG.warn("YES!");
						} else {
//							LOG.warn("NO!");
						}
					} catch (NonMatchingTupleException e) {
						// should not happen!
//						LOG.error(e.getMessage());
						e.printStackTrace();
					}
				}
			}
		}

		// guard existance output
		if (outputGuard) {
			context.write(value, value);
//			LOG.warn(value.toString() + " " + value.toString());
		} else {
			// guarded existance output
			for (GFAtomicExpression guarded : getGuardedsAll()) {
				if (guarded.matches(t)) {
					context.write(value, value);
//					LOG.warn(value.toString() + " " + value.toString());
					break;
				}
			}
		}

		return;

	}
}
