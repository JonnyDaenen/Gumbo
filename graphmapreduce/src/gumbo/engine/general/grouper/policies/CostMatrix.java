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
	}


	public void putCost(CalculationGroup job1, CalculationGroup job2, CalculationGroup cost) {
		combinations[getIndex(job1)][getIndex(job2)] = cost;

	}

	private int getIndex(CalculationGroup job1) {
		for (int i = 0; i < jobs.length; i++) {
			if (jobs[i].equals(job1))
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
		combinations[getIndex(job1)][getIndex(job2)] = null;
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

		for (; i < combinations.length; i++) {
			for (; j < combinations[i].length; j++) {
				CalculationGroup job = combinations[i][j];
				if (job != null && job.getCost() > maxCost) {
					maxCost = job.getCost();
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
				System.out.print(job.getCost());
			}
			
			System.out.println();
		}
	}
}
