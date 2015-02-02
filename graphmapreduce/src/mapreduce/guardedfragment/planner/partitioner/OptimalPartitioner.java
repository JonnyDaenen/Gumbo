/**
 * Created: 29 Apr 2014
 */
package mapreduce.guardedfragment.planner.partitioner;

import mapreduce.guardedfragment.planner.calculations.CalculationUnit;
import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;
import cern.colt.Arrays;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class OptimalPartitioner implements CalculationPartitioner {

	/**
	 * @see mapreduce.guardedfragment.planner.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCalculationUnitDAG partition(CalculationUnitDAG set) {

		// TODO check implementation

		// create array of calculations
		CalculationUnit[] calculations = set.getCalculations().toArray(new CalculationUnit[set.size()]);
		System.out.println(Arrays.toString(calculations));

		return findBestLevelAssignment(set, calculations, new int [calculations.length], 0, 0, calculations.length-1, null);
	}


	protected PartitionedCalculationUnitDAG getBest(CalculationUnit [] calculations, int [] levelassignment, PartitionedCalculationUnitDAG currentBest) {

		// construct partition
		PartitionedCalculationUnitDAG candidate = new PartitionedCalculationUnitDAG(calculations, levelassignment);

		// check if the dependencies are ok 
		// and all levels are occupied
		if (!candidate.checkDependencies() || !candidate.allLevelsUsed())
			return currentBest;

		// if this is the first one
		if (currentBest == null) {
			calculateScore(candidate); // TODO remove
			return candidate;
		}

		// compare scores and return smallest
		double score1 = calculateScore(candidate);
		double score2 = calculateScore(currentBest);

		if (score1 < score2)
			return candidate;
		else
			return currentBest;
	}



	/** Calculates the score of a partition.
	 * @param candidate a partition
	 * @return the score of a partition
	 */
	private double calculateScore(PartitionedCalculationUnitDAG candidate) {

		int totalSpread = 0;
		for (CalculationUnit c : candidate.getCalculations()) {
			totalSpread += candidate.getSpread(c); // * weight of calculation;
		}

		System.out.println(candidate);
		System.out.println(totalSpread);
		return totalSpread;
	}


	protected PartitionedCalculationUnitDAG findBestLevelAssignment(CalculationUnitDAG set, CalculationUnit [] calculations, int [] levelassignment, int nextItem, int minLevel, int maxLevel, PartitionedCalculationUnitDAG currentBest) {

		if(nextItem == levelassignment.length) {
			System.out.println(Arrays.toString(levelassignment));
			return getBest(calculations, levelassignment, currentBest);
		} else {
			CalculationUnit c = calculations[nextItem];
			// TODO check definition of height and depth
			for (int i = c.getHeight() - 1; i <= maxLevel - set.getDepth(c) + 1; i++) {
				levelassignment[nextItem] = i;
				currentBest = findBestLevelAssignment(set, calculations, levelassignment, nextItem+1, minLevel, maxLevel, currentBest);
			}
			return currentBest;
		}
	}

}
