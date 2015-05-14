/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round1.mappers;

import gumbo.engine.hadoop.mrcomponents.ParameterPasser;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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

	protected ExpressionSetOperations eso;
	protected HadoopExecutorSettings settings;
	
	Text t;


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
		

		// load parameters
		try {
			ParameterPasser pp = new ParameterPasser(conf);
			eso = pp.loadESO();
			settings = pp.loadSettings();
			
		} catch (Exception e) {
			throw new InterruptedException("Mapper initialisation error: " + e.getMessage());
		}
		
		t = new Text();
	}

	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
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
		t.set(key.toString());
		context.write(t, value);
	}

}
