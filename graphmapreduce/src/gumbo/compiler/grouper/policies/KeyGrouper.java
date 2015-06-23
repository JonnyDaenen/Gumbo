package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.costmodel.CostCalculator;
import gumbo.compiler.grouper.structures.CalculationGroup;
import gumbo.compiler.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.gfexpressions.io.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * Implementation of the greedy grouping algorithm by Wang and Chan (VLDB '14).
 * This is a quick implementation, i.e., some parts can be optimized: re-using calculations,
 * finding the max at construction time, ...
 * 
 * @author Jonny Daenen
 *
 */
public class KeyGrouper implements GroupingPolicy {
	
	
	CostCalculator costCalculator;
	
	
	public KeyGrouper(CostCalculator costCalculator) {
		this.costCalculator = costCalculator;
	}

	@Override
	public List<CalculationGroup> group(
			CalculationGroup group) {

		// init: put every job in its own group

		List<CalculationGroup> groups = new LinkedList<CalculationGroup>();
		for (GuardedSemiJoinCalculation semijoin : group.getAll()){
			CalculationGroup cg = new CalculationGroup();
			cg.add(semijoin);
			groups.add(cg);
		}

		boolean savingsPossible = true;
		
		// repeat until no positive savings
		while (savingsPossible) {	
			
			// create cost savings matrix
			double [][] matrix = createCostMatrix(groups);

			savingsPossible = hasSavings(matrix);

			if (!savingsPossible)
				break;

			// find jobs with highest cost savings
			Pair<Integer,Integer> indices = findHighestSavings(matrix);

			// merge jobs with highest cost savings
			mergeAndRemove(groups, indices.fst, indices.snd);

		}


		return groups;
	}
	
	/**
	 * Creates the group cost matrix, based on the cost function.
	 * 
	 * @param groups
	 * @return
	 */
	private double[][] createCostMatrix(List<CalculationGroup> groups) {
		
		double [][] matrix = new double[groups.size()][groups.size()];
		
		for (int i = 0; i < matrix.length; i++) {
			for (int j = i+1; j < matrix[i].length; j++){
				
				CalculationGroup group1 = groups.get(i);
				CalculationGroup group2 = groups.get(j);
				
				// create merged group
				CalculationGroup newGroup = new CalculationGroup();
				newGroup.addAll(group1);
				newGroup.addAll(group2);
				
				matrix[i][j] = 0;
				matrix[i][j] += costCalculator.calculateCost(group1);
				matrix[i][j] += costCalculator.calculateCost(group2);
				matrix[i][j] -= costCalculator.calculateCost(newGroup);
				
				
			}
		}
		
		return matrix;
	}
	
	/**
	 * checks if a matrix contains savings, i.e. a cost larger than 0.
	 * @param matrix
	 * @return
	 */
	private boolean hasSavings(double[][] matrix) {
		for (int i = 0; i < matrix.length; i++) {
			for (int j = i+1; j < matrix[i].length; j++){
				if (matrix[i][j] > 0)
					return true;
			}
		}
		return false;
	}

	

	/**
	 * Finds the positions of the max value in a 2D rectangular matrix.
	 * @param matrix
	 * @return
	 */
	private Pair<Integer, Integer> findHighestSavings(double[][] matrix) {
		Pair<Integer, Integer> maxPos = new Pair<Integer, Integer>(0, 0);
		double max = 0;
		
		for (int i = 0; i < matrix.length; i++) {
			for (int j = i+1; j < matrix[i].length; j++){
				if (matrix[i][j] > max) {
					max = matrix[i][j];
					maxPos.fst = i;
					maxPos.snd = j;
				}
			}
		}
		return maxPos;
	}

	
	
	private void mergeAndRemove(List<CalculationGroup> groups, int fst,
			int snd) {

		CalculationGroup group1 = groups.get(fst);
		CalculationGroup group2 = groups.get(snd);
		
		// create merged group
		CalculationGroup newGroup = new CalculationGroup();
		newGroup.addAll(group1);
		newGroup.addAll(group2);
		
		// remove from back to front
		groups.remove(Math.max(fst, snd));
		groups.remove(Math.min(fst, snd));
		
		// add new group
		groups.add(newGroup);
		
	}

	

}
