package gumbo.engine.general.grouper.policies;

import java.util.LinkedList;
import java.util.List;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.CostModel;
import gumbo.engine.general.grouper.costmodel.MRSettings;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.sample.Simulator;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.structures.gfexpressions.io.Pair;
import gumbo.utils.estimation.SamplingException;

public class CostBasedGrouper implements GroupingPolicy {


	private RelationFileMapping rfm;
	private CostModel costModel;
	private Simulator simulator;

	private RelationTupleSampleContainer samples;
	private CostMatrix costMatrix;


	public CostBasedGrouper(RelationFileMapping rfm, CostModel costModel) {
		this.rfm = rfm;
		this.costModel = costModel;
	}


	@Override
	public List<CalculationGroup> group(CalculationGroup group) {

		try {
			// sample relations
			fetchSamples();

			// calculate initial costs
			init(group);

			// perform grouping
			return performGrouping(group);
		} catch (SamplingException e) {
			// FIXME exception
			return null;
		}
	}



	private void fetchSamples() throws SamplingException {
		RelationSampler sampler = new RelationSampler(rfm);
		RelationSampleContainer rawSamples = sampler.sample();
		samples = new RelationTupleSampleContainer(rawSamples, 0.1);
	}


	/**
	 * Calculates the costs of the original jobs using the cost model.
	 * @param group
	 */
	private void init(CalculationGroup group) {
		for ( GuardedSemiJoinCalculation calculation : group.getAll()) {

			// create new job
			CalculationGroup calcJob = new CalculationGroup();
			calcJob.add(calculation);

			// calculate intermediate data
			estimateParameters(calcJob);

			// calculate and set cost
			double cost = costModel.calculateCost(calcJob);
			calcJob.setCost(cost);
		}

	}


	private void estimateParameters(CalculationGroup calcJob) {
		// execute algorithm on sample
		SimulatorReport report = simulator.execute(calcJob);


		// fill in parameters 
		calcJob.setGuardInBytes(report.getGuardInBytes());
		calcJob.setGuardedInBytes(report.getGuardedInBytes());
		calcJob.setGuardOutBytes(report.getGuardOutBytes());
		calcJob.setGuardedOutBytes(report.getGuardedOutBytes());

	}


	private List<CalculationGroup> performGrouping(CalculationGroup group) {

		// greedy approach

		while (costMatrix.hasPositiveCost()) {

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
				estimateParameters(newGroupEstimate);
				costMatrix.putCost(existingGroup, newGroup, newGroupEstimate);
			}
			
			
		}

		LinkedList<CalculationGroup> l = new LinkedList<CalculationGroup>(costMatrix.getGroups());
		
		return l;
	}



}
