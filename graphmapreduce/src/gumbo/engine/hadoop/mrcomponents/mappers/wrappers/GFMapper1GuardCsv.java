/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers.wrappers;

import gumbo.engine.hadoop.mrcomponents.tools.RelationResolver;
import gumbo.structures.data.RelationSchema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Outputs the atoms when a guard is projected onto them.
 * The input is in csv format, the output is in relational format.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper1GuardCsv extends GFMapper1GuardRelOptimized {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardCsv.class);

	Text out1 = new Text();
	Text out2 = new Text();
	

	RelationResolver resolver;

	private StringBuilder stringBuilder;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		resolver = new RelationResolver(eso);
		// pre-cache
		resolver.extractRelationSchema(context);

		stringBuilder = new StringBuilder(128);
		
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

			// find out relation name
			RelationSchema rs = resolver.extractRelationSchema(context);

			// trim is necessary to remove extra whitespace
			String t1 = value.toString().trim();
			
			// wrap tuple in relation name
			stringBuilder.setLength(0);
			stringBuilder.append(rs.getName());
			stringBuilder.append('(');
			stringBuilder.append(t1);
			stringBuilder.append(')');
			
			value.set(stringBuilder.toString());
			
			super.map(key, value, context);
			
			} catch (Exception e) {
				LOG.error(e.getMessage());
				e.printStackTrace();
				throw new InterruptedException(e.getMessage());
			}

	}

}
