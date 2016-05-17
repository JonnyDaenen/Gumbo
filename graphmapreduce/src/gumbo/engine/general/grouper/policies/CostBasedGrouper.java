package gumbo.engine.general.grouper.policies;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.grouper.GroupingException;
import gumbo.engine.general.grouper.costmodel.CostModel;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.sample.SimulatorInterface;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.CostMatrix;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.utils.estimation.SamplingException;

public class CostBasedGrouper implements GroupingPolicy {


	private RelationFileMapping rfm;
	private CostModel costModel;
	private AbstractExecutorSettings execSettings;

	private RelationTupleSampleContainer samples;
	private SimulatorInterface simulator;
	private CostMatrix costMatrix;


	public CostBasedGrouper(RelationFileMapping rfm, CostModel costModel, AbstractExecutorSettings execSettings, RelationTupleSampleContainer samples) {
		this.rfm = rfm;
		this.costModel = costModel;
		this.execSettings = execSettings;
		this.samples = samples;
		
		
		
	}


	@Override
	public List<CalculationGroup> group(CalculationGroup group) throws GroupingException {

		try {
			// sample relations
			fetchSamples();

			// calculate initial costs
			init(group);

			// perform grouping
			return performGrouping(group);
		} catch (SamplingException e) {
			throw new GroupingException("Something went wrong during grouping in sampling stage.", e);
		}
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


	/**
	 * Calculates the costs of the original jobs using the cost model.
	 * @param group
	 * @throws GroupingException 
	 */
	private void init(CalculationGroup group) throws GroupingException {

		Set<CalculationGroup> jobs = new HashSet<CalculationGroup>();

		// calculate single costs
		for ( GuardedSemiJoinCalculation calculation : group.getAll()) {

			// create new job
			CalculationGroup calcJob = new CalculationGroup(group.getRelevantExpressions());
			calcJob.add(calculation);
			jobs.add(calcJob);

			// calculate intermediate data
			estimateParameters(calcJob);

		}


		costMatrix = new CostMatrix(jobs);

		// calculate pair costs
		for (Pair<CalculationGroup,CalculationGroup> p : costMatrix.getGroupPairs()) {
			CalculationGroup job1 = p.fst;
			CalculationGroup job2 = p.snd;
			// merge jobs
			CalculationGroup newJob = job1.merge(job2);

			// calculate intermediate data and cost
			estimateParameters(newJob);

			costMatrix.putCost(job1, job2, newJob);


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
			cost = costModel.calculateCost(report);
			System.out.println(report);
		} else {
			cost = costModel.calculateCost(calcJob);
		}
		calcJob.setCost(cost);
		System.out.println(calcJob);

	}


	private List<CalculationGroup> performGrouping(CalculationGroup group) throws GroupingException {

		// greedy approach

		while (costMatrix.hasPositiveCost()) {
//						costMatrix.printMatrix(true);
//						costMatrix.printGroups();

			// pick best merge option
			Pair<CalculationGroup, CalculationGroup> oldGroups = costMatrix.getBestOldGroups();
			CalculationGroup group1 = oldGroups.fst;
			CalculationGroup group2 = oldGroups.snd;


			CalculationGroup newGroup = costMatrix.getBestNewGroup();


			// update cost matrix
			costMatrix.remove(group1);
			costMatrix.remove(group2);
			costMatrix.add(newGroup);



			// calculate new combinations
			for (CalculationGroup existingGroup : costMatrix.getGroups()) {

				CalculationGroup newGroupEstimate = newGroup.merge(existingGroup);

				// calculate intermediate data and cost
				estimateParameters(newGroupEstimate);

				costMatrix.putCost(existingGroup, newGroup, newGroupEstimate);
			}


		}

		LinkedList<CalculationGroup> l = new LinkedList<CalculationGroup>(costMatrix.getGroups());

		return l;
	}



}
