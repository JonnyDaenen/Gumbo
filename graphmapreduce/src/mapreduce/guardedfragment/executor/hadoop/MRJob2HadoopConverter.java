/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.combiners.GFCombiner1;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1GuardCsv;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1GuardRel;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1GuardedCsv;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1GuardedRel;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1Identity;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper2GuardCsv;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper2GuardRel;
import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapperHadoop;
import mapreduce.guardedfragment.executor.hadoop.readers.GuardInputFormat;
import mapreduce.guardedfragment.executor.hadoop.reducers.GFReducer1;
import mapreduce.guardedfragment.executor.hadoop.reducers.GFReducer2;
import mapreduce.guardedfragment.executor.hadoop.reducers.GFReducerHadoop;
import mapreduce.guardedfragment.planner.compiler.DirManager;
import mapreduce.guardedfragment.planner.compiler.mappers.GFMapper1AtomBased;
import mapreduce.guardedfragment.planner.compiler.mappers.GFMapper2Generic;
import mapreduce.guardedfragment.planner.compiler.reducers.GFReducer1AtomBased;
import mapreduce.guardedfragment.planner.compiler.reducers.GFReducer2Generic;
import mapreduce.guardedfragment.planner.structures.InputFormat;
import mapreduce.guardedfragment.planner.structures.MRJob;
import mapreduce.guardedfragment.planner.structures.MRJob.MRJobType;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.operations.ExpressionSetOperations;
import mapreduce.hadoop.readwrite.RelationInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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


	private ExecutorSettings settings;


	private static final Log LOG = LogFactory.getLog(MRJob2HadoopConverter.class);

	/**
	 * 
	 */
	public MRJob2HadoopConverter() {
		serializer = new GFPrefixSerializer();
		settings = new ExecutorSettings(); // TODO add as parameter
	}

	/**
	 * Converts a MRJob to a hadoop ControlledJob.
	 * 
	 * @param job
	 *            the job to convert
	 * @return the Controlledjob
	 */
	public ControlledJob convert(MRJob job, DirManager dirManager) {

		switch (job.getType()) {

		case GF_ROUND1:
			return createRound1Job(job,dirManager);

		case GF_ROUND2:
			return createRound2Job(job,dirManager);

		default:
			// CLEAN exception
			return null;
		}
	}

	/**
	 * @param job
	 * @param dirManager 
	 * @return
	 */
	private ControlledJob createRound1Job(MRJob job, DirManager dirManager) {
		// create job
		Job hadoopJob;
		try {
			hadoopJob = Job.getInstance();

			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(job.getJobName());

			/* set guard and guarded set */
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(job.getGFExpressions()));
			conf.set("relationfilemapping", dirManager.getFileMapping().toString());

			/* set mapper and reducer */
			//			hadoopJob.setMapperClass(GFMapperHadoop.class);
			//			conf.set("GFMapperClass", GFMapper1AtomBased.class.getCanonicalName());

			//
			//			hadoopJob.setReducerClass(GFReducerHadoop.class); 
			//			conf.set("GFReducerClass", GFReducer1AtomBased.class.getCanonicalName());

			hadoopJob.setReducerClass(GFReducer1.class); 
			ReduceJobEstimator redestimator = new ReduceJobEstimator();
			hadoopJob.setNumReduceTasks(redestimator.getNumReducers(job,dirManager));

			//			hadoopJob.setCombinerClass(GFCombiner1.class);

			/* set IO */
			//			// code for 1 mapper
			//			for (Path inpath : job.getInputPaths()) {
			//				System.out.println("Setting path" + inpath);
			//				FileInputFormat.addInputPath(hadoopJob, inpath);
			//			}

			// 2 separate mappers:

			ExpressionSetOperations eso = new ExpressionSetOperations();
			eso.setExpressionSet(job.getGFExpressions());
			eso.setDirManager(dirManager);



			for ( Path guardedPath : eso.getGuardedRelPaths()) {
				LOG.info("Setting M1 guarded path to " + guardedPath + " using mapper " + GFMapper1GuardedRel.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardedPath, 
						TextInputFormat.class, GFMapper1GuardedRel.class);
			}

			for ( Path guardedPath : eso.getGuardedCsvPaths()) {
				LOG.info("Setting M1 guarded path to " + guardedPath + " using mapper " + GFMapper1GuardedCsv.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardedPath, 
						TextInputFormat.class, GFMapper1GuardedCsv.class);
			}

			// WARNING: equal guarded paths are overwritten!
			for ( Path guardPath : eso.getGuardRelPaths()) {
				LOG.info("Setting M1 guard path to " + guardPath + " using mapper " + GFMapper1GuardRel.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardPath, 
						TextInputFormat.class, GFMapper1GuardRel.class);
			}

			for ( Path guardPath : eso.getGuardCsvPaths()) {
				LOG.info("Setting M1 guard path to " + guardPath + " using mapper " + GFMapper1GuardCsv.class.getName());
				MultipleInputs.addInputPath(hadoopJob, guardPath, 
						TextInputFormat.class, GFMapper1GuardCsv.class);
			}




			// Deprecated!
			// do not use default paths
			// default input path means is send to old mapper
			Set<Path> gPaths = eso.getGuardPaths();
			for (Path path : gPaths) {
				if(path.equals(dirManager.getDefaultInputPath())) {
					LOG.info("Default input path detected (->redirecting): " + path);
					MultipleInputs.addInputPath(hadoopJob, path, 
							RelationInputFormat.class, 
							GFMapperHadoop.class);
					conf.set("GFMapperClass", GFMapper1AtomBased.class.getCanonicalName());
					break;
				}
			}


			FileOutputFormat.setOutputPath(hadoopJob, job.getOutputPath());

			// set intermediate/mapper output
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(Text.class);

			// set reducer output
			// hadoopJob.setOutputKeyClass(NullWritable.class);
			hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(IntWritable.class);


			// TODO check
			//			hadoopJob.setInputFormatClass(RelationInputFormat.class);

			return new ControlledJob(hadoopJob, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GFOperationInitException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * @param job
	 * @param dirManager 
	 * @return
	 */
	private ControlledJob createRound2Job(MRJob job, DirManager dirManager) {

		// create job
		Job hadoopJob;
		try {
			hadoopJob = Job.getInstance();

			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(job.getJobName());

			// set guard and guarded set
			Configuration conf = hadoopJob.getConfiguration();
			conf.set("formulaset", serializer.serializeSet(job.getGFExpressions()));
			conf.set("relationfilemapping", dirManager.getFileMapping().toString());


			/* set mapper and reducer */
			if (settings.getBooleanProperty(ExecutorSettings.guardKeepaliveOptimizationOn)) {
				// add special non-identity mapper to process the guard input again
				
				ExpressionSetOperations eso = new ExpressionSetOperations();
				eso.setExpressionSet(job.getGFExpressions());
				eso.setDirManager(dirManager);
				
				// direct them to the special mapper (rel)
				for (Path guardPath : eso.getGuardRelPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + GFMapper2GuardRel.class.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, GFMapper2GuardRel.class);
				}
				
				// direct them to the special mapper (csv)
				for (Path guardPath : eso.getGuardCsvPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + GFMapper2GuardCsv.class.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, GFMapper2GuardCsv.class);
				}
				
				// other files just need to be read and pushed to the reducer
				for (Path inpath : job.getInputPaths()) {
					LOG.info("Adding M2 normal path " + inpath + " using identity mapper ");
					MultipleInputs.addInputPath(hadoopJob, inpath, 
							GuardInputFormat.class, Mapper.class);
				}
			} else {
				hadoopJob.setMapperClass(Mapper.class);

				for (Path inpath : job.getInputPaths()) {
					LOG.info("Adding M2 path " + inpath + " using identity mapper ");
					FileInputFormat.addInputPath(hadoopJob, inpath);
				}
				
				// TODO check
				// we use a custom input class to allow the mapper to output key-value pairs again
				hadoopJob.setInputFormatClass(GuardInputFormat.class);
			}
			//			hadoopJob.setMapperClass(GFMapperHadoop.class);
			//			conf.set("GFMapperClass", GFMapper2Generic.class.getCanonicalName());

			//			hadoopJob.setReducerClass(GFReducerHadoop.class);
			//			conf.set("GFReducerClass", GFReducer2Generic.class.getCanonicalName());
			hadoopJob.setReducerClass(GFReducer2.class);

			/* set output */
			FileOutputFormat.setOutputPath(hadoopJob, job.getOutputPath());


			// set intermediate/mapper output
			hadoopJob.setMapOutputKeyClass(Text.class);
			hadoopJob.setMapOutputValueClass(IntWritable.class); // OPTIMIZE make it a list? for combiner

			// set reducer output
			hadoopJob.setOutputKeyClass(NullWritable.class);
			//			hadoopJob.setOutputKeyClass(Text.class);
			hadoopJob.setOutputValueClass(Text.class);

			

			return new ControlledJob(hadoopJob, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GFOperationInitException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}

}
