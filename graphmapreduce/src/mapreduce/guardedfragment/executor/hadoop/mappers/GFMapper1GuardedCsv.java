/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.InputFormat;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.RelationFileMappingException;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.planner.structures.data.RelationSchemaException;
import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Also outputs the atoms when a guarded atom is projected onto them.
 * Only allows tuples from 1 relation as input.
 * 
 * @author Jonny Daenen
 * 
 */

public class GFMapper1GuardedCsv extends GFMapper1Identity {

	private static final Log LOG = LogFactory.getLog(GFMapper1GuardedCsv.class);
	RelationFileMapping rm;
	
	/**
	 * @see mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1Identity#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {

		
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		
		// get relation name
		String relmapping = conf.get("relationfilemapping");
		LOG.error(relmapping);
		try {
			FileSystem fs = FileSystem.get(conf);
			rm = new RelationFileMapping(relmapping,fs);
			LOG.error(rm.toString());

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
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(org.apache.hadoop.io.Text,
	 *      org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// find out relation name
		try {
			// TODO optimize
			
			InputSplit is = context.getInputSplit();
			Method method;
			method = is.getClass().getMethod("getInputSplit");
			method.setAccessible(true);
			FileSplit fileSplit = (FileSplit) method.invoke(is);
			Path filePath = fileSplit.getPath();
			
//			LOG.error("File Name: "+filePath);
			
			RelationSchema rs = rm.findSchema(filePath);
			
			
			String t = rs.getName() + "(" + value.toString() + ")";
			Text tuple = new Text(t);
			
			context.write(tuple, tuple);
			// System.out.println(t);

			
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		

	}


}
