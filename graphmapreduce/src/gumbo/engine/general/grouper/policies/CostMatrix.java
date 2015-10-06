package gumbo.engine.general.grouper.policies;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * A cost matrix for job combinations. 
 * The matrix cannot grow larger than its initial size!
 * 
 * @author Jonny Daenen
 *
 */
public class CostMatrix {

	CalculationGroup [] jobs;
	CalculationGroup [][] combinations;

	public CostMatrix(Set<CalculationGroup> jobs) {
		this.jobs = jobs.toArray(new CalculationGroup[0]);
		this.combinations = new CalculationGroup[this.jobs.length][this.jobs.length];
	}


	public void putCost(CalculationGroup job1, CalculationGroup job2, CalculationGroup cost) {
		int i = getIndex(job1);
		int j = getIndex(job2);
		combinations[i][j] = cost;
		combinations[j][i] = cost;

	}

	private int getIndex(CalculationGroup job) {
		for (int i = 0; i < jobs.length; i++) {
			if (jobs[i] != null && jobs[i].equals(job))
				return i;
		}
		return -1;
	}

	private CalculationGroup getJob(int index) {
		return jobs[index];
	}


	public CalculationGroup getCost(CalculationGroup job1, CalculationGroup job2) {
		return combinations[getIndex(job1)][getIndex(job2)];
	}

	public void removeEntry(CalculationGroup job1, CalculationGroup job2) {
		putCost(job1, job2, null);
	}

	public void remove(CalculationGroup job) {
		int i = getIndex(job);
		if (i < 0)
			return;

		jobs[i] = null;
	}

	/**
	 * Adds a job to the matrix.
	 * If the matrix is full, the job is not added.
	 * @param job
	 */
	public void add(CalculationGroup job) {
		for (int i = 0; i < jobs.length; i++) {
			if (jobs[i] == null){
				jobs[i] = job;
				return;
			}

		}
	}


	public boolean hasPositiveCost() {
		return getBestOldGroupsIndex() != null;
	}

	public Pair<CalculationGroup,CalculationGroup> getBestOldGroups() {
		Pair<Integer,Integer> result = getBestOldGroupsIndex();
		if (result == null)
			return null;
		else
			return new Pair<>(getJob(result.fst), getJob(result.snd));
	}




	private Pair<Integer,Integer> getBestOldGroupsIndex() {
		double maxCost = 0;
		int maxi = 0, maxj = 0;
		boolean found = false;
		int i = 0, j = 0;

		for (i = 0; i < combinations.length; i++) {
			for (j = 0; j < combinations[i].length; j++) {
				if (!exists(i,j))
					continue;

				CalculationGroup job = combinations[i][j];

				double savings = getCostSavings(getJob(i), getJob(j), job);
				if (savings > maxCost) {
					maxCost = savings;
					maxi = i;
					maxj = j;
					found = true;
				}

			}
		}

		if (!found)
			return null;
		else
			return new Pair<>(maxi,maxj);
	}

	private boolean exists(int i, int j) {
		return i != j && getJob(i) != null && getJob(j) != null && combinations[i][j] != null;
	}


	public CalculationGroup getBestNewGroup() {
		Pair<Integer,Integer> result = getBestOldGroupsIndex();
		if (result == null)
			return null;
		else
			return combinations[result.fst][result.snd];
	}

	public Collection<CalculationGroup> getGroups() {
		HashSet<CalculationGroup> result = new HashSet<CalculationGroup>();

		for (int i = 0; i < jobs.length; i++) {
			if (jobs[i] != null)
				result.add(jobs[i]);
		}

		return result;
	}


	public void printMatrix() {

		for (int i = 0; i < combinations.length; i++) {
			for (int j = 0 ; j < combinations[i].length; j++) {
				CalculationGroup job = combinations[i][j];
				if (!exists(i,j)) {
					System.out.print("---\t");
					continue;
				}
				double savings = getCostSavings(getJob(i), getJob(j), job);
				System.out.print(savings + "\t");
			}

			System.out.println();
		}
		
		System.out.println("Legend:");
		

		for (int i = 0; i < jobs.length; i++) {
			if (jobs[i] != null)
				System.out.println(i + ":" + jobs[i]);
			
		}
		
		for (int i = 0; i < combinations.length; i++) {
			for (int j = 0 ; j < combinations[i].length; j++) {
				CalculationGroup job = combinations[i][j];
				if (!exists(i,j)) {
					continue;
				}
				double savings = getCostSavings(getJob(i), getJob(j), job);
				System.out.println(i + "," + j + ":" + savings + System.lineSeparator() + combinations[i][j]);
				System.out.println(i + ":" + jobs[i]);
				System.out.println(j + ":" + jobs[j]);
				System.out.println("---");
			}

			System.out.println();
		}
		
		int a = 0;
	}


	public Collection<Pair<CalculationGroup,CalculationGroup>> getGroupPairs() {
		HashSet<Pair<CalculationGroup,CalculationGroup>> result = new HashSet<>();

		for (int i = 0; i < combinations.length; i++) {
			if (jobs[i] == null)
				continue;
			for (int j = i+1 ; j < combinations[i].length; j++) {
				if (jobs[i] == null)
					continue;
				result.add(new Pair<>(getJob(i),getJob(j)));
			}
		}

		return result;
	}


	public double getCostSavings(CalculationGroup job1, CalculationGroup job2, CalculationGroup combinedJob) {

		return job1.getCost() + job2.getCost() - combinedJob.getCost();
	}
}
