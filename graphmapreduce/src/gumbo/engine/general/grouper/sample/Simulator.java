package gumbo.engine.general.grouper.sample;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.algorithms.Map1GuardAlgorithm;
import gumbo.engine.general.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.general.algorithms.MapAlgorithm;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.messagefactories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.hadoop.reporter.FakeMapper;
import gumbo.engine.hadoop.reporter.LinearExtrapolator;
import gumbo.engine.hadoop.reporter.RelationReport;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;


/**
 * Simulates a run of the first Map job in Gumbo.
 * Results are summarized in a report.
 * 
 * @author Jonny Daenen
 *
 */
public class Simulator {


	RelationFileMapping mapping;
	AbstractExecutorSettings settings;
	RelationTupleSampleContainer rtsc;
	LinearExtrapolator extrapolator;
	ExpressionSetOperations eso;


	public Simulator(RelationTupleSampleContainer rtsc, RelationFileMapping mapping, AbstractExecutorSettings settings) {
		this.mapping = mapping;
		this.settings = settings;
		this.rtsc = rtsc;
		this.extrapolator = new LinearExtrapolator();
	}

	public SimulatorReport execute(CalculationGroup calcJob) {

		SimulatorReport report = new SimulatorReport();
		
		try {
			eso = new ExpressionSetOperations(calcJob.getExpressions(), calcJob.getRelevantExpressions(), mapping);

			// for each input relation
			for (RelationSchema r : calcJob.getInputRelations()) {

				// calculate size
				long byteSize = mapping.getRelationSize(r);

				// run simulation and
				// attribute to correct part (guard vs. guarded)
				if (isGuard(r)) {
					long intermediate = runGuard(r, calcJob);
					report.addGuardInBytes(byteSize);
					report.addGuardOutBytes(intermediate);
				} else {
					long intermediate = runGuarded(r, calcJob);
					report.addGuardedInBytes(byteSize);
					report.addGuardedOutBytes(intermediate);
				}

			}
		} catch (GFOperationInitException | AlgorithmInterruptedException e) {
			// FIXME throw error
			e.printStackTrace();
		}

		return report;
	}



	private boolean isGuard(RelationSchema r) {
		return eso.isGuard(r);
	}

	private long runGuard(RelationSchema r, CalculationGroup calcJob) throws AlgorithmInterruptedException {
		return runSamples(r, calcJob, true);
	}

	private long runGuarded(RelationSchema rs, CalculationGroup calcJob) throws AlgorithmInterruptedException {
		return runSamples(rs, calcJob, false);
	}

	private long runSamples(RelationSchema rs, CalculationGroup calcJob, boolean guard) throws AlgorithmInterruptedException {

		// simulate small sample
		long smallOutput = runOneSample(rtsc.getSmallTuples(rs), guard);

		// simulate big sample
		long bigOutput = runOneSample(rtsc.getBigTuples(rs), guard);

		// extrapolate
		extrapolator.loadValues(rtsc.getSmallSize(rs), smallOutput, rtsc.getBigSize(rs), bigOutput);
		return (long)extrapolator.extrapolate(mapping.getRelationSize(rs));

	}

	private long runOneSample(Iterable<Tuple> tuples, boolean guard) throws AlgorithmInterruptedException {
		FakeMapper fm = new FakeMapper();

		MapAlgorithm algo;
		if (guard) { // TODO make eso predicate
			Map1GuardMessageFactoryInterface fact = new Map1GuardMessageFactory(fm.context, settings, eso);
			fact.enableSampleCounting();
			algo = new Map1GuardAlgorithm(eso, fact, settings);
		} else {
			Map1GuardedMessageFactoryInterface fact = new Map1GuardedMessageFactory(fm.context, settings, eso);
			fact.enableSampleCounting();
			algo = new Map1GuardedAlgorithm(eso,fact,settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn));
		}

		feedTuples(algo, tuples);

		return (long) fm.context.getOutputBytes();
	}

	private void feedTuples(MapAlgorithm algo, Iterable<Tuple> tuples) throws AlgorithmInterruptedException {
		long offset = 0;
		for (Tuple tuple : tuples) {
			// feed to algorithm
			algo.run(tuple, offset);
			offset += tuple.size(); // dummy offset
		}
	}

}
