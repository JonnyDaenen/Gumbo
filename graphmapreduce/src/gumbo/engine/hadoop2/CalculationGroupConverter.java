package gumbo.engine.hadoop2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.grouper.Grouper;
import gumbo.engine.general.grouper.GrouperFactory;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.sample.Simulator;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.general.utils.FileMappingExtractor;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.evaluate.EvaluateMapper;
import gumbo.engine.hadoop2.mapreduce.evaluate.EvaluateReducer;
import gumbo.engine.hadoop2.mapreduce.evaluate.IdentityMapper;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateMapper;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateReducer;
import gumbo.engine.hadoop2.mapreduce.tools.PropertySerializer;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.utils.estimation.SamplingException;

public class CalculationGroupConverter {

	private static final Log LOG = LogFactory.getLog(CalculationGroupConverter.class);

	private GumboPlan plan;
	private Configuration conf;
	private FileManager fm;
	private FileMappingExtractor extractor;


	private RelationTupleSampleContainer samples;
	private RelationSampleContainer rawSamples;



	public CalculationGroupConverter(GumboPlan plan, Configuration conf) {
		this.plan = plan;
		this.conf = conf;
		this.fm = plan.getFileManager();

		this.extractor = new FileMappingExtractor();
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

			int numRed =  (int) Math.max(1,group.getGuardedOutBytes() / (128*1024*1024.0) ); // FIXME extract from settings
			hadoopJob.setNumReduceTasks(numRed); 
			LOG.info("Setting VAL Reduce tasks to " + numRed);


			// SETTINGS
			// set map output types
			hadoopJob.setMapOutputKeyClass(VBytesWritable.class);
			hadoopJob.setMapOutputValueClass(GumboMessageWritable.class);

			// set reduce output types
			hadoopJob.setOutputKeyClass(VBytesWritable.class);
			hadoopJob.setOutputValueClass(GumboMessageWritable.class);
			hadoopJob.setOutputFormatClass(SequenceFileOutputFormat.class);


			// set output path

			// register path for all semi-joins
			HashSet<String> labels = new HashSet<>();
			Set<GFExistentialExpression> calculations = new HashSet<>();
			for (GuardedSemiJoinCalculation sj : group.getAll()) {
				String label = sj.getCanonicalString();
				labels.add(label);

				calculations.add(sj.getExpression());
			}
			Path intermediatePath = fm.getNewTmpPath(group.getCanonicalName(), labels);

			FileOutputFormat.setOutputPath(hadoopJob, intermediatePath);


			// pass settings to mapper
			configure(hadoopJob, calculations);


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
			hadoopJob.setJobName(plan.getName() + "_EVAL_" + partition.getCanonicalOutString());

			// MAPPER
			// couple guard input files
			Set<GFExistentialExpression> calculations = new HashSet<>();
			for (CalculationUnit cu : partition.getCalculations()) {

				BasicGFCalculationUnit bgfcu = (BasicGFCalculationUnit) cu;
				calculations.add(bgfcu.getBasicExpression());

				// guard input paths
				RelationSchema rs = bgfcu.getGuardRelations().getRelationSchema();
				Set<Path> paths = fm.getFileMapping().getPaths(rs);
				for(Path path : paths) {
					LOG.info("Adding guard path:" + path);
					MultipleInputs.addInputPath(hadoopJob, path, 
							TextInputFormat.class, EvaluateMapper.class);
				}

				// intermediate semi-joins results
				for (GuardedSemiJoinCalculation sj : bgfcu.getSemiJoins()) {  
					String canon = sj.getCanonicalString();
					Path intermediatePath = fm.getReference(canon);
					LOG.info("Adding intermediate path:" + intermediatePath);
					MultipleInputs.addInputPath(hadoopJob, intermediatePath, 
							SequenceFileInputFormat.class, Mapper.class);
				}
			}


			// REDUCER
			hadoopJob.setReducerClass(EvaluateReducer.class); 

			int numRed =  30; // FIXME calculate sum of sizes
			hadoopJob.setNumReduceTasks(numRed); 
			LOG.info("Setting EVAL Reduce tasks to " + numRed);

			// SETTINGS
			// set map output types
			hadoopJob.setMapOutputKeyClass(VBytesWritable.class);
			hadoopJob.setMapOutputValueClass(GumboMessageWritable.class);

			// set reduce output types
			hadoopJob.setOutputKeyClass(NullWritable.class);
			hadoopJob.setOutputValueClass(Text.class);


			// set output path base (subdirs will be made)
			// FIXME this should be followed by a move when job is done
			Path dummyPath = fm.getOutputRoot().suffix("/"+partition.getCanonicalOutString());
			FileOutputFormat.setOutputPath(hadoopJob, dummyPath);


			// pass settings
			configure(hadoopJob, calculations);

			joblist.add(new ControlledJob(hadoopJob, null));


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return joblist;

	}

	public List<CalculationGroup> group(CalculationUnitGroup partition) {

		// sample new relations
		updateSamples();

		// extract necessary files
		// TODO why is this necessary?
		extractor.setIncludeOutputDirs(true);
		RelationFileMapping mapping = extractor.extractFileMapping(fm); 

		// get the correct grouper
		HadoopExecutorSettings settings = new HadoopExecutorSettings(conf);
		Grouper grouper = GrouperFactory.createGrouper(mapping, settings, samples); // FIXME correct mapping

		// apply grouping
		List<CalculationGroup> groups = grouper.group(partition);

		// add missing sample data if necessary
		// create a job for each group
		for (CalculationGroup group : groups) {

			if (!group.hasInfo()) {
				LOG.info("Missing size estimates, sampling data.");
				addInfo(group, mapping, settings);
			}
		}

		return groups;
	}

	private void addInfo(CalculationGroup group, RelationFileMapping mapping, HadoopExecutorSettings settings) {
		// execute algorithm on sample
		Simulator simulator = new Simulator(samples, fm.getFileMapping(), settings);
		SimulatorReport report = simulator.execute(group);

		// fill in parameters 
		group.setGuardInBytes(report.getGuardInBytes());
		group.setGuardedInBytes(report.getGuardedInBytes());
		group.setGuardOutBytes(report.getGuardOutBytes());
		group.setGuardedOutBytes(report.getGuardedOutBytes());
	}

	private void updateSamples() {
		try {
			if (this.rawSamples == null) 
				rawSamples = new RelationSampleContainer();

			// resolve paths
			extractor.setIncludeOutputDirs(true);
			RelationFileMapping mapping = extractor.extractFileMapping(fm);

			RelationSampler sampler = new RelationSampler(mapping);
			sampler.sample(rawSamples);

			if (this.samples == null) {
				samples = new RelationTupleSampleContainer(rawSamples, 0.1);
			} else {
				samples.update(rawSamples);
			}
		} catch (SamplingException e) {
			LOG.error("Could not sample: " + e.getMessage());
			e.printStackTrace();
			LOG.warn("Trying to continue without sampling, possibly too few reducers will be allocated.");
		}
	}

	private void configure(Job hadoopJob, Set<GFExistentialExpression> expressions) {
		Configuration conf = hadoopJob.getConfiguration();

		// queries
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		conf.set("gumbo.queries", serializer.serializeSet(expressions));

		// output mapping
		HashMap<String, String> outmap = new HashMap<>();
		for (RelationSchema rs: fm.getOutFileMapping().getSchemas()) {
			for (Path path : fm.getOutFileMapping().getPaths(rs)) {
				outmap.put(rs.getName(), path.toString());
			}
		}
		String outmapString = PropertySerializer.objectToString(outmap);
		conf.set("gumbo.outmap", outmapString);

		// file to id mapping
		HashMap<String, Long> fileidmap = new HashMap<>();
		HashMap<Long, String> idrelmap = new HashMap<>();

		// resolve paths
		extractor.setIncludeOutputDirs(true);
		RelationFileMapping mapping = extractor.extractFileMapping(fm);

		List<Pair<RelationSchema, Path>> rspath = new ArrayList<>();
		// for each rel
		for (RelationSchema rs : mapping.getSchemas()) {
			// determine paths
			Set<Path> paths = mapping.getPaths(rs);
			for (Path p : paths) {
				rspath.add(new Pair<>(rs,p));
			}

		}

		Comparator<Pair<RelationSchema, Path>> comp = new Comp();
		Collections.sort(rspath, comp );

		long i = 0;
		for (Pair<RelationSchema, Path> p : rspath) {
			fileidmap.put(p.snd.toString(), i);
			idrelmap.put(i,p.fst.getName());
			i++;
		}

		String fileidmapString = PropertySerializer.objectToString(fileidmap);
		conf.set("gumbo.fileidmap", fileidmapString);


		// file id to relation name mapping
		String idrelmapString = PropertySerializer.objectToString(idrelmap);
		conf.set("gumbo.filerelationmap", idrelmapString);

		// atom-id mapping
		HashMap<String,Integer> atomidmap = new HashMap<>();
		int maxatomid = 0;
		for (GFExistentialExpression exp : expressions) {
			for (GFAtomicExpression atom : exp.getAtomic()) {
				int id = plan.getAtomId(atom);
				atomidmap.put(atom.toString(), id);
				maxatomid = Math.max(id, maxatomid);
			}
		}

		String atomidmapString = PropertySerializer.objectToString(atomidmap);
		conf.set("gumbo.atomidmap", atomidmapString);


		// maximal atom id
		conf.setInt("gumbo.maxatomid", maxatomid);
	}

	public void moveOutputFiles(CalculationUnitGroup partition) throws IOException {
		
		

		FileSystem dfs = FileSystem.get(conf);
		
		for ( RelationSchema rs:  partition.getOutputRelations()) {
			Path from = fm.getOutputRoot().suffix(Path.SEPARATOR + partition.getCanonicalOutString() + Path.SEPARATOR + rs.getName()+ "-r-*");
			Path to = fm.getOutFileMapping().getPaths(rs).iterator().next();
			
			dfs.mkdirs(to);
			FileStatus[] files = dfs.globStatus(from);
			for(FileStatus file: files) {
				if (!file.isDirectory()) {
					LOG.info("Moving files: " + from + " " + to);
					dfs.rename(file.getPath(), to);
				}
				// TODO perform merge
			}
			
		}

		



	}


}
