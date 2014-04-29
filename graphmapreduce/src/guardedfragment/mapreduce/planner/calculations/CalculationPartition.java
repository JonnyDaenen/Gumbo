/**
 * Created: 29 Apr 2014
 */
package guardedfragment.mapreduce.planner.calculations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Represents a DAG of calculations. The leafs are the starting points of calculation, the roots the endpoints.
 * 
 * @author Jonny Daenen
 *
 */
public class CalculationPartition implements Iterable<CalculationUnit> {
	
	Set<CalculationUnit> calculations;
	Set<CalculationUnit> leafs;
	Set<CalculationUnit> roots;
	

	public CalculationPartition() {
		calculations = new HashSet<CalculationUnit>();
		leafs = new HashSet<CalculationUnit>();
		roots = new HashSet<CalculationUnit>();
	}
	
	
	/**
	 * Adds a finished calculation to the partition. All dependencies should be set correctly,
	 * because while adding it is determined whether this is a root/leaf/... 
	 * @param c
	 */
	public void addCalculation(CalculationUnit c) {
		calculations.add(c);
		
		// add to leaf set if applicable
		if(c.isLeaf()) {
			leafs.add(c);
		}
		
		// add to root set if applicable
		boolean isRoot = true;
		
		for (CalculationUnit oldCalc : calculations) {
			
			if (oldCalc.getDependencies().contains(c)){
				isRoot = false;
				break;
			}
		}
		if (isRoot) {
			roots.add(c);
		}
		
		// remove bad roots
		Collection<CalculationUnit> deps = c.getDependencies();
		for (CalculationUnit root : roots) {
			if(deps.contains(root)) {
				roots.remove(root);
			}
		}
		
		
	}
	
	public Iterable<CalculationUnit> getLeafs(){
		return leafs;
	}
	
	public Iterable<CalculationUnit> getRoots(){
		return roots;
	}
	
	/**
	 * Calculates the maximal height of a component in the DAG.
	 * @return the maximal height of a component in the DAG
	 */
	public int getHeight() {
		int max = 0;
		for (CalculationUnit root : roots) {
			max = Math.max(max, root.getHeight());
		}
		
		return max;
		
	}


	/**
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<CalculationUnit> iterator() {
		return calculations.iterator();
	}


	/**
	 * @param height
	 * @return partition with all calculations of the specified height
	 */
	public CalculationPartition getCalculationsByHeight(int height) {
		CalculationPartition cp = new CalculationPartition();
		
		for (CalculationUnit c : calculations) {
			if(c.getHeight() == height)
				cp.addCalculation(c);
		}
		
		return cp;
	}


	/**
	 * @return
	 */
	public int size() {
		return calculations.size();
	}

	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "Partition: {"+ System.lineSeparator();
		for (CalculationUnit c : calculations) {
			s += c + System.lineSeparator();
		}
		s += "}";
		
		return s;
	}
}