package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.costmodel.CostCalculator;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.input.parser.GumboGFQueryVisitor;
import gumbo.structures.gfexpressions.io.Pair;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Implementation of the greedy grouping algorithm by Wang and Chan (VLDB '14).
 * This is a quick implementation, i.e., some parts can be optimized: re-using calculations,
 * finding the max at construction time, ...
 * 
 * @author Jonny Daenen
 *
 */
public class KeyGrouper implements GroupingPolicy {


	private static final Log LOG = LogFactory.getLog(KeyGrouper.class);
	
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
			
			logcostmatrix(groups, matrix);
			

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
	
	private void logcostmatrix(List<CalculationGroup> groups, double[][] matrix) {
		

		String s = "";
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[i].length; j++){
				s += (matrix[i][j]) + "\t";
			}
			s += System.lineSeparator();
		}
		
		LOG.info("\n" + s);

		LOG.info("---");
		
		for (int i = 0; i < matrix.length; i++) {
			for (int j = i+1; j < matrix[i].length; j++){
				LOG.info(groups.get(i) + " MERGE WITH" + groups.get(j) + ": " + matrix[i][j]);
				
			}
		}

		LOG.info("---");
		
		
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
				
				LOG.info("Trying to group" );
				
				// create merged group
				CalculationGroup newGroup = new CalculationGroup();
				newGroup.addAll(group1);
				newGroup.addAll(group2);
				
				matrix[i][j] = 0;
				double cost1 = costCalculator.calculateCost(group1);
				double cost2 = costCalculator.calculateCost(group2);
				double cost3 = costCalculator.calculateCost(newGroup);
				

				LOG.info(group1);
				LOG.info("Cost: " + cost1);
				LOG.info(group2);
				LOG.info("Cost: " + cost2);
				LOG.info(newGroup);
				LOG.info("Cost: " + cost3);
				
				matrix[i][j] = cost1 + cost2 - cost3;
				LOG.info("Improvement: " + matrix[i][j]);
				LOG.info("Improvement %: " + (matrix[i][j] / (cost1 + cost2))*100);
				
				
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
