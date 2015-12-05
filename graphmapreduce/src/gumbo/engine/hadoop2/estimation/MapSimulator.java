package gumbo.engine.hadoop2.estimation;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapreduce.Mapper;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.algorithms.Map1GuardAlgorithm;
import gumbo.engine.general.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.general.algorithms.MapAlgorithm;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.messagefactories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.hadoop.reporter.FakeMapper;
import gumbo.engine.hadoop.reporter.LinearExtrapolator;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.estimation.DummyMapper.DummyContext;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateMapper;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;


/**
 * Simulates a run of the a Map job.
 * Results are summarized in a report.
 * 
 * @author Jonny Daenen
 *
 */
public class MapSimulator {

	private static final Log LOG = LogFactory.getLog(MapSimulator.class);


	RelationFileMapping mapping;
	AbstractExecutorSettings settings;
	RelationTupleSampleContainer rtsc;
	LinearExtrapolator extrapolator;
	ExpressionSetOperations eso;


	public MapSimulator(RelationTupleSampleContainer rtsc, RelationFileMapping mapping) {
		this.mapping = mapping;
		this.rtsc = rtsc;
		this.extrapolator = new LinearExtrapolator();
	}

	public SimulatorReport execute(Collection<RelationSchema> inputRelations, Configuration configuration) throws AlgorithmInterruptedException {

		SimulatorReport report = new SimulatorReport();

		// for each input relation
		for (RelationSchema r : inputRelations) {

			//				if (LOG.isDebugEnabled())
			LOG.info("Simulating relation " + r);


			// run simulation and calculate intermediate output
			runSamples(r, configuration, report); 

			

		}

		//		System.out.println(report);
		return report;
	}



	private void runSamples(RelationSchema rs, Configuration configuration, SimulatorReport report) throws AlgorithmInterruptedException {

		// simulate small sample
		Pair<Boolean, Long> result1 = runOneSample(rs, rtsc.getSmallTuples(rs), configuration);
		long smallOutput = result1.snd;

		// simulate big sample
		Pair<Boolean, Long> result2 = runOneSample(rs, rtsc.getBigTuples(rs), configuration);
		long bigOutput = result2.snd;

		// extrapolate
		long inputBytes = mapping.getRelationSize(rs);
		extrapolator.loadValues(rtsc.getSmallSize(rs), smallOutput, rtsc.getBigSize(rs), bigOutput);
		long intermediateBytes =  (long)extrapolator.extrapolate(inputBytes);

		boolean guard = result2.fst;
		if (guard) {
			report.addGuardInBytes(inputBytes);
			report.addGuardOutBytes(intermediateBytes);
		} else {
			report.addGuardedInBytes(inputBytes);
			report.addGuardedOutBytes(intermediateBytes);
		}
		
		LOG.info("Map Input bytes:" + inputBytes);
		LOG.info("Est. Map Output bytes: " + intermediateBytes);
		

	}

	private Pair<Boolean,Long> runOneSample(RelationSchema rs, Iterable<Tuple> tuples, Configuration configuration) throws AlgorithmInterruptedException {
		DummyMapper map = new DummyMapper();
		DummyContext context = map.getContext(configuration, tuples, rs.getName());

		Mapper<LongWritable,Text,VBytesWritable,GumboMessageWritable> mapfunction = new ValidateMapper();

		try {
			mapfunction.run(context);
		} catch (IOException | InterruptedException e) {
			LOG.error("Something went wrong during mapper simulation! Trying to continue...", e);
		}

		return new Pair<>(context.getRequestBytes() != 0,(long) context.getKeyBytes() + context.getValueBytes());
	}


}
