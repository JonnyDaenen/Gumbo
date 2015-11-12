/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;

import java.util.Arrays;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class OptimalPartitioner implements CalculationPartitioner {

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCUGroup partition(CalculationUnitGroup set, FileManager fm) {

		// TODO check implementation

		// create array of calculations
		CalculationUnit[] calculations = set.getCalculations().toArray(new CalculationUnit[set.size()]);
//		System.out.println(Arrays.toString(calculations));

		return findBestLevelAssignment(set, calculations, new int [calculations.length], 0, 0, calculations.length-1, null);
	}


	protected PartitionedCUGroup getBest(CalculationUnit [] calculations, int [] levelassignment, PartitionedCUGroup currentBest) {

		// construct partition
		PartitionedCUGroup candidate = new PartitionedCUGroup(calculations, levelassignment);

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
	private double calculateScore(PartitionedCUGroup candidate) {

		int totalSpread = candidate.getSpread();

		System.out.println(candidate);
		System.out.println(totalSpread);
		return totalSpread;
	}


	protected PartitionedCUGroup findBestLevelAssignment(CalculationUnitGroup set, CalculationUnit [] calculations, int [] levelassignment, int nextItem, int minLevel, int maxLevel, PartitionedCUGroup currentBest) {

		if(nextItem == levelassignment.length) {
//			System.out.println(Arrays.toString(levelassignment));
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
