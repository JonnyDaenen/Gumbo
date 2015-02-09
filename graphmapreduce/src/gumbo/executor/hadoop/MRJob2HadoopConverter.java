/**
 * Created: 22 Aug 2014
 */
package gumbo.executor.hadoop;

import gumbo.compiler.resolver.DirManager;
import gumbo.compiler.resolver.mappers.GFMapper1AtomBased;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.compiler.structures.operations.MRJob;
import gumbo.executor.hadoop.input.GuardInputFormat;
import gumbo.executor.hadoop.input.GuardTextInputFormat;
import gumbo.executor.hadoop.mappers.GFMapper1GuardCsv;
import gumbo.executor.hadoop.mappers.GFMapper1GuardRel;
import gumbo.executor.hadoop.mappers.GFMapper1GuardedCsv;
import gumbo.executor.hadoop.mappers.GFMapper1GuardedRel;
import gumbo.executor.hadoop.mappers.GFMapper2GuardCsv;
import gumbo.executor.hadoop.mappers.GFMapper2GuardRel;
import gumbo.executor.hadoop.mappers.GFMapper2GuardTextCsv;
import gumbo.executor.hadoop.mappers.GFMapper2GuardTextRel;
import gumbo.executor.hadoop.mappers.GFMapperHadoop;
import gumbo.executor.hadoop.reducers.GFReducer1;
import gumbo.executor.hadoop.reducers.GFReducer2;
import gumbo.executor.hadoop.reducers.GFReducer2Text;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;
import gumbo.guardedfragment.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.util.Set;

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
			ReduceJobEstimator redestimator = new ReduceJobEstimator(settings);
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


			// TODO are the guard paths filtered??
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




			// TODO Deprecated!
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
							TextInputFormat.class, getRound2GuardMapperClass("rel"));
				}
				
				// direct them to the special mapper (csv)
				for (Path guardPath : eso.getGuardCsvPaths()) {
					LOG.info("Adding M2 guard path " + guardPath + " using mapper " + GFMapper2GuardCsv.class.getName());
					MultipleInputs.addInputPath(hadoopJob, guardPath, 
							TextInputFormat.class, getRound2GuardMapperClass("csv"));
				}
				
				// other files just need to be read and pushed to the reducer
				for (Path inpath : job.getInputPaths()) {
					LOG.info("Adding M2 normal path " + inpath + " using identity mapper ");
					MultipleInputs.addInputPath(hadoopJob, inpath, 
							getRound2MapInputFormat(), Mapper.class);
				}
			} else {
				hadoopJob.setMapperClass(Mapper.class);

				for (Path inpath : job.getInputPaths()) {
					LOG.info("Adding M2 path " + inpath + " using identity mapper ");
					FileInputFormat.addInputPath(hadoopJob, inpath);
				}
				
				// TODO check
				// we use a custom input class to allow the mapper to output key-value pairs again
				hadoopJob.setInputFormatClass(getRound2MapInputFormat());
			}
			//			hadoopJob.setMapperClass(GFMapperHadoop.class);
			//			conf.set("GFMapperClass", GFMapper2Generic.class.getCanonicalName());

			//			hadoopJob.setReducerClass(GFReducerHadoop.class);
			//			conf.set("GFReducerClass", GFReducer2Generic.class.getCanonicalName());
			if (settings.getBooleanProperty(settings.guardTuplePointerOptimizationOn)) {
				hadoopJob.setReducerClass(GFReducer2Text.class);
			} else {
				hadoopJob.setReducerClass(GFReducer2.class);
			}

			/* set output */
			FileOutputFormat.setOutputPath(hadoopJob, job.getOutputPath());


			// set intermediate/mapper output
			hadoopJob.setMapOutputKeyClass(Text.class);
			if (settings.getBooleanProperty(settings.guardTuplePointerOptimizationOn)) {
				hadoopJob.setMapOutputValueClass(Text.class); // OPTIMIZE make it a combined class
			} else {
				hadoopJob.setMapOutputValueClass(IntWritable.class); // OPTIMIZE make it a list? for combiner
			}
			

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

	/**
	 * @param string
	 * @return
	 */
	private Class<? extends Mapper> getRound2GuardMapperClass(String string) {
		
		if (string.equals("csv") ) {
				if (settings.getBooleanProperty(settings.guardTuplePointerOptimizationOn)) {
					return GFMapper2GuardTextCsv.class;
				} else {
					return GFMapper2GuardCsv.class;
				}
		} else {
			if (settings.getBooleanProperty(settings.guardTuplePointerOptimizationOn)) {
				return GFMapper2GuardTextRel.class;
			} else {
				return GFMapper2GuardRel.class;
			}
		}
	}

	/**
	 * @return
	 */
	private Class<? extends org.apache.hadoop.mapreduce.InputFormat> getRound2MapInputFormat() {
		Class<? extends org.apache.hadoop.mapreduce.InputFormat> atomInputFormat = GuardInputFormat.class;
		if (settings.getBooleanProperty(settings.guardTuplePointerOptimizationOn)) {
			atomInputFormat = GuardTextInputFormat.class;
		}
		return atomInputFormat;
	}

}
