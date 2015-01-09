/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.InputFormat;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
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
public class GFMapper1Identity extends Mapper<LongWritable, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(GFMapper1Identity.class);

	ExpressionSetOperations eso;

	/**
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// load context
		super.setup(context);
		Configuration conf = context.getConfiguration();


		String s = String.format("Mapper"+this.getClass().getSimpleName()+"-%05d-%d",
		        context.getTaskAttemptID().getTaskID().getId(),
		        context.getTaskAttemptID().getId());
		LOG.info(s);

		GFPrefixSerializer serializer = new GFPrefixSerializer();

		// load guard
		try {
			HashSet<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}
			
			eso = new ExpressionSetOperations();
			eso.setExpressionSet(formulaSet);

		} catch (Exception e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}
		
		
		
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
//		InputSplit is = context.getInputSplit();
//		Method method = is.getClass().getMethod("getInputSplit");
//		method.setAccessible(true);
//		FileSplit fileSplit = (FileSplit) method.invoke(is);
//		Path filePath = fileSplit.getPath();
//		
//		LOG.error("File Name Processing "+filePath);
//		
		context.write(new Text(key.toString()), value);
	}

}
