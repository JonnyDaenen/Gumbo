package gumbo.engine.general.grouper.policies;

import java.util.LinkedList;
import java.util.List;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.CostModel;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.sample.Simulator;
import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.utils.estimation.SamplingException;

public class BestCostBasedGrouper implements GroupingPolicy {


	private RelationFileMapping rfm;
	private CostModel costModel;
	private AbstractExecutorSettings execSettings;

	private RelationTupleSampleContainer samples;
	private Simulator simulator;
	
	private List<CalculationGroup> bestGrouping;
	private double bestTotalCost;

	public BestCostBasedGrouper(RelationFileMapping rfm, CostModel costModel, AbstractExecutorSettings execSettings) {
		this.rfm = rfm;
		this.costModel = costModel;
		this.execSettings = execSettings;
	}


	@Override
	public List<CalculationGroup> group(CalculationGroup group) {

		try {
			// sample relations
			fetchSamples();
			
			// find best grouping
			findBest(group);
			
			// return best grouping
			return bestGrouping;
			
		} catch (SamplingException e) {
			// FIXME exception
			return null;
		}
	}



	private void findBest(CalculationGroup group) {
		List<CalculationGroup> unitjobs = new LinkedList<CalculationGroup>();

		// calculate single costs
		for ( GuardedSemiJoinCalculation calculation : group.getAll()) {

			// create new job
			CalculationGroup calcJob = new CalculationGroup(group.getRelevantExpressions());
			calcJob.add(calculation);
			unitjobs.add(calcJob);

		}
		

		bestTotalCost = totalCost(unitjobs);
		bestGrouping = unitjobs;
		
		
		findBestGroupingRec(unitjobs, new LinkedList<CalculationGroup>());
		
	}
	


	private void findBestGroupingRec(List<CalculationGroup> unitjobs, List<CalculationGroup> candidate) {
		
		if (unitjobs.size() == 0){
			double newCost = totalCost(candidate);
			System.out.println("Candidate solution found! " + " " + newCost);
			if (newCost < bestTotalCost) {
				bestTotalCost = newCost;
				bestGrouping = candidate;
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
				newSolution.add(newCg);
				
				findBestGroupingRec(unitjobs, newSolution);
			}
			
			
			unitjobs.add(0, head);
		}
		
		
	}


	private double totalCost(List<CalculationGroup> jobs) {
		
		double totalCost = 0;
		for (CalculationGroup cg : jobs) {
			// estimate intermediate data and calculate cost
			estimateParameters(cg);
			totalCost += cg.getCost();
		}
		
		return totalCost;
	}


	private void fetchSamples() throws SamplingException {
		RelationSampler sampler = new RelationSampler(rfm);
		RelationSampleContainer rawSamples = sampler.sample();
		samples = new RelationTupleSampleContainer(rawSamples, 0.1);

		simulator = new Simulator(samples, rfm, execSettings);
	}


	


	private void estimateParameters(CalculationGroup calcJob) {
		// execute algorithm on sample
		SimulatorReport report = simulator.execute(calcJob);


		// fill in parameters 
		calcJob.setGuardInBytes(report.getGuardInBytes());
		calcJob.setGuardedInBytes(report.getGuardedInBytes());
		calcJob.setGuardOutBytes(report.getGuardOutBytes());
		calcJob.setGuardedOutBytes(report.getGuardedOutBytes());

		// calculate and set cost
		double cost = costModel.calculateCost(calcJob);
		calcJob.setCost(cost);

	}


}
