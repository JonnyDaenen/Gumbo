package gumbo.engine.hadoop2.estimation;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.grouper.sample.SimulatorInterface;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.LinearExtrapolator;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.hadoop2.converter.Configurator;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.estimation.DummyMapper.DummyContext;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateMapper;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;


/**
 * Simulates a run of the a Map job.
 * Results are summarized in a report.
 * 
 * @author Jonny Daenen
 *
 */
public class MapSimulator implements SimulatorInterface {

	private static final Log LOG = LogFactory.getLog(MapSimulator.class);


	RelationFileMapping mapping;
	AbstractExecutorSettings settings;
	RelationTupleSampleContainer rtsc;
	LinearExtrapolator extrapolator;

	Class<? extends Mapper<?,?,?,?>> mapClass;


	public MapSimulator() {
		mapClass = ValidateMapper.class;
	}
	
	public void setMapClass(Class<? extends Mapper<?, ?, ?, ?>> map) {
		mapClass = map;
	}

	@Override
	public void setInfo(RelationTupleSampleContainer rtsc, RelationFileMapping mapping,
			AbstractExecutorSettings execSettings) {
		this.mapping = mapping;
		this.rtsc = rtsc;
		this.settings = execSettings;

		this.extrapolator = new LinearExtrapolator();
	}

	@Override
	public SimulatorReport execute(CalculationGroup calcJob) throws AlgorithmInterruptedException {
		Configuration conf = createConfig(calcJob);
		return execute(calcJob.getInputRelations(), conf);
	}


	private SimulatorReport execute(Collection<RelationSchema> inputRelations, Configuration conf) throws AlgorithmInterruptedException {

		SimulatorReport report = new SimulatorReport();

		// for each input relation
		for (RelationSchema r : inputRelations) {

			//				if (LOG.isDebugEnabled())
			LOG.info("Simulating relation " + r);


			// run simulation and calculate intermediate output
			runSamples(r, report, conf); 

		}

		//		System.out.println(report);
		return report;
	}



	private void runSamples(RelationSchema rs, SimulatorReport report, Configuration conf) throws AlgorithmInterruptedException {

		// simulate small sample
		SimResult result1 = runOneSample(rs, rtsc.getSmallTuples(rs), conf);

		// simulate big sample
		SimResult result2 = runOneSample(rs, rtsc.getBigTuples(rs), conf);

		// extrapolate map output size
		long inputBytes = mapping.getRelationSize(rs);
		extrapolator.loadValues(rtsc.getSmallSize(rs), result1.bytes, rtsc.getBigSize(rs), result2.bytes);
		long intermediateBytes =  (long)extrapolator.extrapolate(inputBytes);
		
		// extrapolate map output tuples
		extrapolator.loadValues(result1.bytes, result1.records, result2.bytes, result2.records);
		long intermRec =  (long)extrapolator.extrapolate(intermediateBytes);
		
		// extrapolate map input tuples
		extrapolator.loadValues(rtsc.getSmallSize(rs), result1.inRecords, rtsc.getBigSize(rs), result2.inRecords);
		long inRec =  (long)extrapolator.extrapolate(inputBytes);
		
		
		if (result2.guard) {
			report.addGuardDetails(inputBytes, (long) intermediateBytes, inRec, intermRec);
		} else {
			report.addGuardedDetails(inputBytes, (long) intermediateBytes, inRec, intermRec);
		}

		LOG.info("Map Input bytes:" + inputBytes);
		LOG.info("Est. Map Output bytes: " + intermediateBytes);


	}

	private SimResult runOneSample(RelationSchema rs, Iterable<Tuple> tuples, Configuration conf) throws AlgorithmInterruptedException {
		DummyMapper map = new DummyMapper();
		DummyContext context = map.getContext(conf, tuples, rs.getName());
		
		// create and run mapper
		try {
			Mapper<LongWritable,Text,VBytesWritable,GumboMessageWritable> mapfunction = (Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>) mapClass.newInstance();
			mapfunction.run(context);
		} catch (IOException | InterruptedException | InstantiationException | IllegalAccessException e) {
			LOG.error("Something went wrong during mapper simulation! Trying to continue...", e);
		}

		//		System.out.println("Num assert:" + context.getAssertBytes());
		//		System.out.println("Num req:" + context.getRequestBytes());
		//		System.out.println("Num key:" + context.getKeyBytes());
		//		System.out.println("Num val:" + context.getValueBytes());

		SimResult result = new SimResult();
		result.guard = context.getRequestBytes() != 0;
		result.bytes = context.getOutBytes();
		result.records = context.getNumRecords();
		result.inRecords = context.getNumInRecords();
		return result;
	}

	private Configuration createConfig(CalculationGroup calcJob) {
		Configuration conf;
		HadoopExecutorSettings set = (HadoopExecutorSettings)settings;
		conf = new Configuration(set.getConf());
		Configurator.addQueries(conf, calcJob.getExpressions());


		return conf;
	}
	
	public class SimResult {
		public boolean guard = false;
		public long bytes;
		public long records;
		public long inRecords;
	}



}
