/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Also outputs the atoms when a guarded atom is projected onto them.
 * Accepts tuples from multiple relations, in rel-format.
 * 
 * @author Jonny Daenen
 * 
 */

public class GFMapper1GuardedRel extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedRel.class);

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		boolean print = false;
		if (value.toString().contains("(59)")) {
			LOG.error("Mapper1: " + value);
			print = true;
		}

		// trim is necessary to remove extra whitespace
		value.set(value.toString().trim());
		Tuple t = new Tuple(value.toString());
		// System.out.println(t);

		// guarded existance output
		for (GFAtomicExpression guarded : eso.getGuardedsAll()) {

			// if no guarded expression matches this tuple, it will not be output
			if (guarded.matches(t)) {
				context.write(value, value);
				if (print) {
					LOG.error("Mapper1 output: " + value + " " + value);
				}
				//				 LOG.warn("Guard: " + value.toString() + " " + value.toString());
				break;
			}
		}

	}


}
