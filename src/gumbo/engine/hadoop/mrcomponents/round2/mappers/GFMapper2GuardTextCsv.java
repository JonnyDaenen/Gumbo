/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.round2.mappers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.filemapper.RelationFileMappingException;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.RelationSchemaException;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 * 
 */
public class GFMapper2GuardTextCsv extends GFMapper2GuardTextRel {

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(GFMapper2GuardTextCsv.class);
	private RelationFileMapping rm;


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {


		super.setup(context);

		Configuration conf = context.getConfiguration();

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
	}


	/**
	 * @throws InterruptedException
	 * @throws IOException
	 * @see gumbo.compiler.resolver.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		// find out relation name
		// TODO optimize

		try {


			InputSplit is = context.getInputSplit();
			Method method = is.getClass().getMethod("getInputSplit");

			method.setAccessible(true);
			FileSplit fileSplit = (FileSplit) method.invoke(is);
			Path filePath = fileSplit.getPath();

			//			LOG.error("File Name: "+filePath);

			RelationSchema rs = rm.findSchema(filePath);

			// trim is necessary to remove extra whitespace
			String t1 = value.toString().trim();

			t1 = rs.getName() + "(" + t1 + ")";
			value.set(t1);

			super.map(key, value, context);

		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


}
