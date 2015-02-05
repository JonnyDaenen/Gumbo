/**
 * Created: 21 Aug 2014
 */
package gumbo.executor.hadoop.mappers;

import gumbo.compiler.structures.RelationFileMapping;
import gumbo.compiler.structures.RelationFileMappingException;
import gumbo.compiler.structures.data.RelationSchemaException;
import gumbo.executor.hadoop.ExecutorSettings;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;
import gumbo.guardedfragment.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1Identity extends Mapper<LongWritable, Text, Text, Text> {

	private static final Log LOG = LogFactory.getLog(GFMapper1Identity.class);

	ExpressionSetOperations eso;
	ExecutorSettings settings;

	RelationFileMapping rm;

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
		

		// get relation name
		String relmapping = conf.get("relationfilemapping");
		//		LOG.error(relmapping);
		try {
			FileSystem fs = FileSystem.get(conf);
			rm = new RelationFileMapping(relmapping,fs);
			//			LOG.trace(rm.toString());

		} catch (RelationSchemaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RelationFileMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO load settings
		settings = new ExecutorSettings();
		
		
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
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
