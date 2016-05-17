package gumbo.engine.general.grouper.policies;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.grouper.GroupingException;
import gumbo.engine.general.grouper.costmodel.CostModel;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.sample.SimulatorInterface;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.utils.estimation.SamplingException;

/**
 * Round 1 grouper that selects the best partitioning based on a given cost model.
 * The grouper traverses all possible partitionings recursively and keeps track of the best option.
 * It is also possible to use this grouper to select a specific grouping. This can be done by providing
 * the {@link AbstractExecutorSettings.BESTGROUP_STOPINDICATOR} setting. When this value is different from 0,
 * the grouper will pick it up and use it as an offset. E.g. when there are 52 partitions (for 4 items),
 * a value of 3 will select the third partition generated (in a deterministic fashion).
 * 
 * @author Jonny Daenen
 *
 */
public class BestCostBasedGrouper implements GroupingPolicy {

	private static final Log LOG = LogFactory.getLog(BestCostBasedGrouper.class);

	private RelationFileMapping rfm;
	private CostModel costModel;
	private AbstractExecutorSettings execSettings;

	private RelationTupleSampleContainer samples;
	private SimulatorInterface simulator;

	private List<CalculationGroup> bestGrouping;
	private double bestTotalCost;
	private int nr;
	private int stopIndicator = 0;

	public BestCostBasedGrouper(RelationFileMapping rfm, CostModel costModel, AbstractExecutorSettings execSettings, RelationTupleSampleContainer samples) {
		this.rfm = rfm;
		this.costModel = costModel;
		this.execSettings = execSettings;
		this.samples = samples;

		this.stopIndicator = (int) execSettings.getNumProperty(AbstractExecutorSettings.BESTGROUP_STOPINDICATOR, 0);

	}


	@Override
	public List<CalculationGroup> group(CalculationGroup group) throws GroupingException {

		try {
			// sample relations
			fetchSamples();

			// find best grouping
			findBest(group);

			// return best grouping
			return bestGrouping;

		} catch (SamplingException e) {
			throw new GroupingException("Something went wrong during grouping in sampling stage.", e);
		}
	}



	private void findBest(CalculationGroup group) throws GroupingException {
		List<CalculationGroup> unitjobs = new LinkedList<CalculationGroup>();

		// calculate single costs
		for ( GuardedSemiJoinCalculation calculation : group.getAll()) {

			// create new job
			CalculationGroup calcJob = new CalculationGroup(group.getRelevantExpressions());
			calcJob.add(calculation);
			unitjobs.add(calcJob);

		}

		nr = 0;
		bestTotalCost = totalCost(unitjobs);
		bestGrouping = unitjobs;


		findBestGroupingRec(unitjobs, new LinkedList<CalculationGroup>());

	}



	private void findBestGroupingRec(List<CalculationGroup> unitjobs, List<CalculationGroup> candidate) throws GroupingException {

		if (stopIndicator != 0 && nr >= stopIndicator) {
			return;
		}

		if (unitjobs.size() == 0){

			// we only calculate something when
			// a. stop == 0, which means we will compare all
			// b. stop == nr, which means we are interested only in the current solution
			nr++;
			if (stopIndicator == 0 || nr == stopIndicator) {
				double newCost = totalCost(candidate);
				LOG.info("Candidate solution found! " + nr + " " + newCost);
				LOG.info(candidate);

				if (newCost < bestTotalCost) {
					bestTotalCost = newCost;
					bestGrouping = candidate;
				}

				if (nr == stopIndicator) {
					bestTotalCost = newCost;
					bestGrouping = candidate;
					return;
				}
			}

		} else {
			CalculationGroup head = unitjobs.remove(0);

			// add separately
			List<CalculationGroup> newSolution = new LinkedList<CalculationGroup>(candidate);
			newSolution.add(head);
			findBestGroupingRec(unitjobs, newSolution);

			// merge with each existing group
			for (CalculationGroup cg : candidate) {
				CalculationGroup newCg = cg.merge(head);

				newSolution = new LinkedList<CalculationGroup>(candidate);
				newSolution.remove(cg);
				newSolution.add(candidate.indexOf(cg), newCg);

				findBestGroupingRec(unitjobs, newSolution);
			}


			unitjobs.add(0, head);
		}
	}


	private double totalCost(List<CalculationGroup> jobs) throws GroupingException {

		double totalCost = 0;
		for (CalculationGroup cg : jobs) {
			// estimate intermediate data and calculate cost
			estimateParameters(cg);
			totalCost += cg.getCost();
		}

		return totalCost;
	}


	private void fetchSamples() throws SamplingException, GroupingException {
		if (samples == null) {
			RelationSampler sampler = new RelationSampler(rfm);
			RelationSampleContainer rawSamples = sampler.sample();
			samples = new RelationTupleSampleContainer(rawSamples, 0.1);
		}
		
		try {
			String className = execSettings.getProperty(execSettings.SIMULATOR_CLASS);
			simulator = (SimulatorInterface) this.getClass().getClassLoader().loadClass(className).newInstance();
			simulator.setInfo(samples, rfm, execSettings);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new GroupingException("Failed to instantiate a simulator", e);
		}
	}


	private void estimateParameters(CalculationGroup calcJob) throws GroupingException {
		// execute algorithm on sample
		SimulatorReport report;
		try {
			report = simulator.execute(calcJob);
		} catch (AlgorithmInterruptedException e) {
			throw new GroupingException("", e);
		}

		// fill in parameters 
		calcJob.setGuardInBytes(report.getGuardInBytes());
		calcJob.setGuardedInBytes(report.getGuardedInBytes());
		calcJob.setGuardOutBytes(report.getGuardOutBytes());
		calcJob.setGuardedOutBytes(report.getGuardedOutBytes());

		// calculate and set cost
		double cost = 0;
		if (report.hasDetails()) {
			System.out.println(report);
			cost = costModel.calculateCost(report);
		} else {
			cost = costModel.calculateCost(calcJob);
		}
		calcJob.setCost(cost);

	}


}
