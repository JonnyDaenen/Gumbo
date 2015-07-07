package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.costmodel.CostCalculator;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.io.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	private Map<Pair<CalculationGroup,CalculationGroup>,Double> costTable;
	private Map<Pair<CalculationGroup,CalculationGroup>,CalculationGroup> merges;


	public KeyGrouper(CostCalculator costCalculator) {
		this.costCalculator = costCalculator;
		costTable = new HashMap<>();
		merges = new HashMap<>();
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

		initCostTable(groups);
		logCostTable(groups);
		boolean savingsPossible = hasSavings();


		// repeat until no positive savings
		while (savingsPossible) {	

			// find jobs with highest cost savings
			Pair<CalculationGroup, CalculationGroup> indices = findHighestSavings();

			// merge jobs with highest cost savings
			updateCostTable(groups, indices.fst, indices.snd);

			// log
			logCostTable(groups);
			savingsPossible = hasSavings();
		}


		return groups;
	}

	private void initCostTable(List<CalculationGroup> groups) {

		LOG.info("Initializing cost table..." );

		costTable.clear();
		merges.clear();

		// initial group cost
		for (int i = 0; i < groups.size(); i++) {
			CalculationGroup group = groups.get(i);
			group.setCost(costCalculator.calculateCost(group));
		}

		for (int i = 0; i < groups.size(); i++) {
			for (int j = i+1; j < groups.size(); j++){

				CalculationGroup group1 = groups.get(i);
				CalculationGroup group2 = groups.get(j);


				// create merged group
				CalculationGroup newGroup = new CalculationGroup();
				newGroup.addAll(group1);
				newGroup.addAll(group2);


				calculateSavings(costTable, group1, group2, newGroup);

			}
		}

	}

	private double calculateSavings(Map<Pair<CalculationGroup, CalculationGroup>, Double> costTable, CalculationGroup group1, CalculationGroup group2, CalculationGroup newGroup) {

		double cost3 = costCalculator.calculateCost(newGroup);
		newGroup.setCost(cost3);

		double cost1 = group1.getCost();
		double cost2 = group2.getCost();
		double savings = cost1 + cost2 - cost3;

		costTable.put(new Pair<CalculationGroup,CalculationGroup>(group1,group2), savings);
		// symmetry, for ease of removal later
		costTable.put(new Pair<CalculationGroup,CalculationGroup>(group2,group1), savings);

		// cache group
		// TODO remove when parent jobs are gone
		merges.put(new Pair<>(group1, group2), newGroup);
		merges.put(new Pair<>(group2, group1), newGroup);

		return savings;
	}

	private void updateCostTable(List<CalculationGroup> groups, CalculationGroup group1, CalculationGroup group2) {


		// remove 
		groups.remove(group1);
		groups.remove(group2);


		// remove old combo's
		Set<Pair<CalculationGroup, CalculationGroup>> removals = new HashSet<>();
		for (Pair<CalculationGroup, CalculationGroup> key : costTable.keySet()) {
			if (key.fst.equals(group1) || key.fst.equals(group2) || key.snd.equals(group1) || key.snd.equals(group2) )
				removals.add(key);
		}

		for (Pair<CalculationGroup, CalculationGroup> key : removals) {
			costTable.remove(key);
		}


		// fetch cached merged group
		CalculationGroup newGroup = merges.get(new Pair<>(group1,group2));

		// create all combos with the new group
		for (CalculationGroup group : groups) {
			// create merged group
			CalculationGroup mergedGroup = new CalculationGroup();
			mergedGroup.addAll(group);
			mergedGroup.addAll(newGroup);

			calculateSavings(costTable, newGroup,group,mergedGroup);

		}


		// add new group
		groups.add(newGroup);


	}

	private void logCostTable(List<CalculationGroup> groups) {

		String leftAlignFormatS = "| %-15s ";
		String leftAlignFormat = "| %15.2f ";
		String leftAlignFormatO = "|!%15.2f)";

		String s = "";

		// total
		// header
		printHeader(groups);
		
		for (int i = 0; i < groups.size(); i++) {
			System.out.format(leftAlignFormatS, getGroupName(groups.get(i)));
			for (int j = 0; j < groups.size(); j++){

				Pair<CalculationGroup, CalculationGroup> cellid = new Pair<CalculationGroup,CalculationGroup>(groups.get(i),groups.get(j));
				//				s += cellid.fst + "," + cellid.snd + ": ";
				s += (costTable.get(cellid)) + "\t";

				if (i == j)
					System.out.format(leftAlignFormatO, groups.get(i).getCost());
				else if (i > j)
					System.out.format(leftAlignFormatS, "-");
				else
					System.out.format(leftAlignFormat, merges.get(cellid).getCost());
			}
			System.out.printf("%n");
			s += System.lineSeparator();
		}
		System.out.printf("%n");


		// delta
		printHeader(groups);
		for (int i = 0; i < groups.size(); i++) {
			System.out.format(leftAlignFormatS, getGroupName(groups.get(i)));
			for (int j = 0; j < groups.size(); j++){

				Pair<CalculationGroup, CalculationGroup> cellid = new Pair<CalculationGroup,CalculationGroup>(groups.get(i),groups.get(j));
				//				s += cellid.fst + "," + cellid.snd + ": ";
				s += (costTable.get(cellid)) + "\t";
				if (i == j)
					System.out.format(leftAlignFormatS, "-");
				else if (i > j)
					System.out.format(leftAlignFormatS, "-");
				else
					System.out.format(leftAlignFormat, costTable.get(cellid));
			}
			System.out.printf("%n");
			s += System.lineSeparator();
		}
		System.out.printf("%n");

		//		LOG.info("\n" + s);


	}

	private void printHeader(List<CalculationGroup> groups) {
		String leftAlignFormatS = "| %-15s ";
		String leftAlignFormat = "| %15.2f ";
		String leftAlignFormatO = "|!%15.2f)";
		System.out.format(leftAlignFormatS, "");
		for (int i = 0; i < groups.size(); i++) 
			System.out.format(leftAlignFormatS, getGroupName(groups.get(i)));
		System.out.printf("%n");
		
	}

	private String getGroupName(CalculationGroup calculationGroup) {
		String name = "";
		for (GuardedSemiJoinCalculation calc : calculationGroup.getAll()) {
			name += calc.getGuarded();

		}
		return name;
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

		//		LOG.info("---");
		//
		//		for (int i = 0; i < matrix.length; i++) {
		//			for (int j = i+1; j < matrix[i].length; j++){
		//				LOG.info(groups.get(i) + " MERGE WITH" + groups.get(j) + ": " + matrix[i][j]);
		//
		//			}
		//		}
		//
		//		LOG.info("---");


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

				//				LOG.info("Trying to group" );

				// create merged group
				CalculationGroup newGroup = new CalculationGroup();
				newGroup.addAll(group1);
				newGroup.addAll(group2);

				matrix[i][j] = 0;
				double cost1 = costCalculator.calculateCost(group1);
				double cost2 = costCalculator.calculateCost(group2);
				double cost3 = costCalculator.calculateCost(newGroup);


				//				LOG.info(group1);
				//				LOG.info("Cost: " + cost1);
				//				LOG.info(group2);
				//				LOG.info("Cost: " + cost2);
				//				LOG.info(newGroup);
				//				LOG.info("Cost: " + cost3);
				//
				matrix[i][j] = cost1 + cost2 - cost3;
				//				LOG.info("Improvement: " + matrix[i][j]);
				//				LOG.info("Improvement %: " + (matrix[i][j] / (cost1 + cost2))*100);


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
	 * checks if a matrix contains savings, i.e. a cost larger than 0.
	 * @return
	 */
	private boolean hasSavings() {
		for (Double val : costTable.values()) {
			if (val > 0)
				return true;
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

	/**
	 * Finds the combination of groups that yield the highest savings.
	 * If no savings are possible, null is returned.
	 * @return
	 */
	private Pair<CalculationGroup, CalculationGroup> findHighestSavings() {
		Pair<CalculationGroup, CalculationGroup> maxPos = null;
		double max = 0;

		for (Pair<CalculationGroup, CalculationGroup> key : costTable.keySet()) {
			double val = costTable.get(key);	
			if (val > max) {
				max = val;
				maxPos = new Pair<>(key.fst,key.snd);
			}
		}
		return maxPos;
	}



	private void mergeAndRemove(List<CalculationGroup> groups, int fst,
			int snd) {

		// remove from back to front
		CalculationGroup group1 = groups.remove(Math.max(fst, snd));
		CalculationGroup group2 = groups.remove(Math.min(fst, snd));

		// create merged group
		CalculationGroup newGroup = new CalculationGroup();
		newGroup.addAll(group1);
		newGroup.addAll(group2);



		// add new group
		groups.add(newGroup);

	}



}
