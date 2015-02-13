/**
 * Created: 21 Aug 2014
 */
package gumbo.engine.hadoop.mrcomponents.mappers;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Also outputs the atoms when a guarded atom is projected onto them.
 * Only allows tuples from 1 relation as input.
 * 
 * @author Jonny Daenen
 * 
 */

public class GFMapper1GuardedCsv extends GFMapper1GuardedRel {

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedCsv.class);


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

			//				LOG.error("File Name: "+filePath);

			RelationSchema rs = eso.getFileMapping().findSchema(filePath);

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
