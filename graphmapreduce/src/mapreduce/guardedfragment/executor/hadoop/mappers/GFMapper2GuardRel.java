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

import mapreduce.guardedfragment.executor.hadoop.ExecutorSettings;
import mapreduce.guardedfragment.executor.hadoop.mappers.TupleIDCreator.TupleIDError;
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
import org.apache.hadoop.io.IntWritable;
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
public class GFMapper2GuardRel extends GFMapper2Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper2GuardRel.class);

	IntWritable out1 = new IntWritable();
	Text out2 = new Text();

	private TupleIDCreator pathids;

	/**
	 * @see mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper2Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
		super.setup(context);
		pathids = new TupleIDCreator(rm);
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		value.set(value.toString().trim());

		try {




			Tuple t = new Tuple(value);
			// System.out.println(t);

			// replace value with pointer when optimization is on
			if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
				value.set(pathids.getTupleID(context, key.get())); // key indicates offset in TextInputFormat
			}


			// check guards + atom (keep-alive)
			for (GFAtomicExpression guard : eso.getGuardsAll()) {

				// if the tuple satisfies the guard expression
				if (guard.matches(t)) {

					int guardID = eso.getAtomId(guard);

					// output guardid
					out1.set(guardID);
					context.write(value, out1);
					context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
					context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(Integer.SIZE/8 + value.getLength());
					
					// output tuple value
					// only when pointer optimization is on
					// to be able to recover the tuple in the reducer
					if (settings.getBooleanProperty(ExecutorSettings.guardTuplePointerOptimizationOn)) {
						out2.set(t.toString());
//						context.write(value, out2); // FIXME change output to text... :-(
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2).increment(1);
						context.getCounter(GumboMap1Counter.KEEP_ALIVE_REQUEST_R2_BYTES).increment(out2.getLength() + value.getLength());
					}




				}
			}

		} catch (SecurityException | GFOperationInitException | TupleIDError e) {
			// should not happen!
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InterruptedException(e.getMessage());
		} 

	}

}
