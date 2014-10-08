/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.IOException;

import mapreduce.guardedfragment.planner.compiler.mappers.GFMapper1AtomBased;
import mapreduce.guardedfragment.planner.compiler.mappers.GFMapper2Generic;
import mapreduce.guardedfragment.planner.compiler.reducers.GFReducer1AtomBased;
import mapreduce.guardedfragment.planner.compiler.reducers.GFReducer2Generic;
import mapreduce.guardedfragment.planner.structures.MRJob;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.hadoop.readwrite.RelationInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A converter for transforming internal MR-jobs into hadoop jobs.
 * 
 * WARNING: Note that only the class description of the map and reduce function
 * are passed. So the concrete instances of these functions are ignored.
 * 
 * @author Jonny Daenen
 * 
 */
public class MRJob2HadoopConverter {

	private GFPrefixSerializer serializer;

	/**
	 * 
	 */
	public MRJob2HadoopConverter() {
		serializer = new GFPrefixSerializer();
	}

	/**
	 * Converts a MRJob to a hadoop ControlledJob.
	 * 
	 * @param job
	 *            the job to convert
	 * @return the Controlledjob
	 */
	public ControlledJob convert(MRJob job) {
		// create job
		Job hadoopJob;
		try {
			hadoopJob = Job.getInstance();

			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(job.getJobName());

			// set IO
			for (Path inpath : job.getInputPaths()) {
				FileInputFormat.addInputPath(hadoopJob, inpath);
			}
			FileOutputFormat.setOutputPath(hadoopJob, job.getOutputPath());

			// set mapper an reducer
			hadoopJob.setMapperClass(GFMapperHadoop.class);
			hadoopJob.setReducerClass(GFReducerHadoop.class);

			// set guard and guarded set
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(job.getGFExpressions()));

			// set correct mapper class
			// OPTIMIZE only the class is passed, so changes to the class are
			// not reflected

			Class mapClass = null;
			Class reduceClass = null;
			
			switch (job.getType()) {
			case GF_ROUND1:
				mapClass = GFMapper1AtomBased.class;
				reduceClass = GFReducer1AtomBased.class;
				break;
			case GF_ROUND2:
//				hadoopJob.setMapperClass(Mapper.class);
				mapClass = GFMapper2Generic.class;
				reduceClass = GFReducer2Generic.class;
				break;
			default:
				// CLEAN exception
				mapClass = job.getMapClass();
				reduceClass = job.getReduceClass();
				break;
			}
			conf.set("GFMapperClass", mapClass.getCanonicalName());
			conf.set("GFReducerClass", reduceClass.getCanonicalName());

			// set intermediate/mapper output
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(Text.class);

			// set reducer output
			// hadoopJob.setOutputKeyClass(NullWritable.class);
			hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(Text.class);

			// TODO check
			hadoopJob.setInputFormatClass(RelationInputFormat.class);

			return new ControlledJob(hadoopJob, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}

}
