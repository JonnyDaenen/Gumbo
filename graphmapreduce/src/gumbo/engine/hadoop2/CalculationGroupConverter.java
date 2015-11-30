package gumbo.engine.hadoop2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.evaluate.EvaluateMapper;
import gumbo.engine.hadoop2.mapreduce.evaluate.EvaluateReducer;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateMapper;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateReducer;
import gumbo.structures.data.RelationSchema;

public class CalculationGroupConverter {

	private static final Log LOG = LogFactory.getLog(CalculationGroupConverter.class);

	private GumboPlan plan;
	private Configuration conf;
	private FileManager fm;

	public CalculationGroupConverter(GumboPlan plan, Configuration conf) {
		this.plan = plan;
		this.conf = conf;
		this.fm = plan.getFileManager();
	}

	public ControlledJob createValidateJob(CalculationGroup group) {


		Job hadoopJob = null;
		try {
			hadoopJob = Job.getInstance(conf); // note: makes a copy of the conf


			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(plan.getName() + "_VAL_"+ group.getCanonicalName());

			// MAPPER
			// couple all input files to mapper
			for (RelationSchema rs : group.getInputRelations()) {
				Set<Path> paths = fm.getFileMapping().getPaths(rs);
				for (Path path : paths) {
					LOG.info("Adding path " + path + " to mapper");
					MultipleInputs.addInputPath(hadoopJob, path, 
							TextInputFormat.class, ValidateMapper.class);
				}
			}

			// REDUCER
			hadoopJob.setReducerClass(ValidateReducer.class); 

			int numRed =  (int) Math.max(1,group.getGuardedOutBytes() / (128*1024*1024.0) );// FIXME extract from settings
			hadoopJob.setNumReduceTasks(numRed); 
			LOG.info("Setting Reduce tasks to " + numRed);


			// SETTINGS
			// set map output types
			hadoopJob.setMapOutputKeyClass(BytesWritable.class);
			hadoopJob.setMapOutputValueClass(GumboMessageWritable.class);

			// set reduce output types
			hadoopJob.setOutputKeyClass(BytesWritable.class);
			hadoopJob.setOutputValueClass(GumboMessageWritable.class);


			// set output path
			Path intermediatePath = fm.getNewTmpPath(group.getCanonicalName());
			// TODO register path for all semi-joins
			FileOutputFormat.setOutputPath(hadoopJob, intermediatePath);


			// TODO pass settings


			return new ControlledJob(hadoopJob, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}

	public List<ControlledJob> createEvaluateJob(CalculationUnitGroup partition) {

		List<ControlledJob> joblist = new ArrayList<>();
		Job hadoopJob;
		try {
			hadoopJob = Job.getInstance(conf); // note: makes a copy of the conf


			hadoopJob.setJarByClass(getClass());
			hadoopJob.setJobName(plan.getName() + "_EVAL_" + partition.getCanonicalOutString()); // TODO add more info

			// MAPPER
			// couple guard input files
			for (CalculationUnit cu : partition.getCalculations()) {

				Path path = null;

				// TODO guard input paths
				MultipleInputs.addInputPath(hadoopJob, path, 
						TextInputFormat.class, EvaluateMapper.class);

				// TODO intermediate semi-joins results
				MultipleInputs.addInputPath(hadoopJob, path, 
						TextInputFormat.class, Mapper.class);

			}


			// REDUCER
			hadoopJob.setReducerClass(EvaluateReducer.class); 

			int numRed =  1; // FIXME calculate sum of sizes
			hadoopJob.setNumReduceTasks(numRed); 
			LOG.info("Setting Reduce tasks to " + numRed);

			// SETTINGS
			// set map output types
			hadoopJob.setMapOutputKeyClass(BytesWritable.class);
			hadoopJob.setMapOutputValueClass(GumboMessageWritable.class);

			// set reduce output types
			hadoopJob.setOutputKeyClass(NullWritable.class);
			hadoopJob.setOutputValueClass(Text.class);


			// set output path base (subdirs will be made)
			Path dummyPath = fm.getOutputRoot().suffix("/"+hadoopJob.getJobName());
			FileOutputFormat.setOutputPath(hadoopJob, dummyPath);


			// TODO pass settings

			joblist.add(new ControlledJob(hadoopJob, null));


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return joblist;

	}

	public List<CalculationGroup> group(CalculationUnitGroup partition) {
		// TODO implement
		return null;
	}

	private void configure(Job hadoopJob) {
		Configuration conf = hadoopJob.getConfiguration();

		// queries
		// TODO conf.set("gumbo.queries", value);

		// output mapping
		// TODO conf.set("gumbo.outmap", value);

		// file to id mapping
		// TODO conf.set("gumbo.fileidmap", value);

		// file id to relation name mapping
		// TODO conf.set("gumbo.filerelationmap", value);

		// atom-id mapping
		// TODO conf.set("gumbo.atomidmap", value);
	}


}
