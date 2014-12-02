/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardRel extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardRel.class);

	Text out1 = new Text();
	Text out2 = new Text();

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		boolean print = false;
		if (value.toString().contains(",59)")) {
			LOG.error("Mapper1: " + value);
			print = true;
		}
		// trim is necessary to remove extra whitespace
		value.set(value.toString().trim());
		
		try {
			
			
			
			Tuple t = new Tuple(value);
			// System.out.println(t);


			boolean outputGuard = false;

			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {
				
				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {
					
					int guardID = eso.getAtomId(guard);
					
					// output guard
					out1.set(t.toString() + ";" + guardID);
					context.write(value, out1);
					// LOG.warn(value.toString() + " " + out1.toString());
					outputGuard = true;

					// projections to atoms
					for (GFAtomicExpression guarded : eso.getGuardeds(guard)) {

						GFAtomProjection p = eso.getProjections(guard, guarded);
						Tuple tprime = p.project(t);
						

						int guardedID = eso.getAtomId(guarded);

						// if the guard projects to this guarded
						// TODO isn't this always the case?
//						if (guarded.matches(tprime)) {
							out1.set(tprime.toString());
							out2.set(t.toString() + ";" + guardedID);
							context.write(out1, out2);
							if (print) {
								LOG.error("Mapper1 output: " + out1 + " " + out2);
							}
//							LOG.warn("Guard: " + out1 + out2);
//						}

					}
				}
			}

			// only output keep-alive if it matched a guard
			if (outputGuard) {
				context.write(value, value);
//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
			}

		} catch (NonMatchingTupleException | GFOperationInitException e) {
			// should not happen!
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  

	}

}
