/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * Partitions the CalculationUnits based on their height in the DAG.
 * @author Jonny Daenen
 *
 */
public class GreedyPartitioner implements CalculationPartitioner {
	
	private static final Log LOG = LogFactory.getLog(CalculationPartitioner.class); 

	public class GreedyLevelQueue {
		
		private Set<CalculationUnit> queue;
		
		private Map<Integer,Set<CalculationUnit>> levels;
		private int lastLevel = -1;
		

		public GreedyLevelQueue(CalculationUnitGroup partition) {
			queue = partition.getCalculations();
			levels = new HashMap<>();
			
		}

		public boolean empty() {
			return queue.isEmpty();
		}

		public Set<CalculationUnit> getActive() {
			
			HashSet<CalculationUnit> active = new HashSet<>();
			
			for (CalculationUnit c : queue) {
				
				// check if a dependency is still in the queue
				boolean add = true;
				for (CalculationUnit dep : c.getDependencies()) {
					if (queue.contains(dep)) {
						add = false;
						break;
					}
				}
				// if not, it becomes active
				if (add) {
					active.add(c);
				}
				
			}
			return active;
		}

		public void level(CalculationUnit cu, Integer level) {
			
			// create level if it doesn't exist
			if (!levels.containsKey(level)) {
				levels.put(level, new HashSet<CalculationUnit>());
			}
			
			// insert calculation
			Set<CalculationUnit> levelset = levels.get(level);
			levelset.add(cu);
			
			// update max level
			lastLevel = Math.max(level, lastLevel);
			
			// remove unit
			queue.remove(cu);
			
		}

		public PartitionedCUGroup createPartitionedDAG() {
			PartitionedCUGroup result = new PartitionedCUGroup();
			
			// from levels from low to high
			for (int level = 0; level <= lastLevel; level++) {
				
				// create partition form current level
				CalculationUnitGroup p = new CalculationUnitGroup();
				
				for (CalculationUnit cu : levels.get(level)) {
					p.add(cu);
				}
					
				// addpartition as new level
				if (p.size() != 0) {
					result.addNewLevel(p);
				}
				
			}
			
			return result;
		}
		
		public Pair<CalculationUnit, Integer> findBestLevelAssignment(Set<CalculationUnit> active) {
			
			int bestOverlap = 0;
			Pair<CalculationUnit, Integer> best = null;
			
			// default assignment
			best = new Pair<>(active.iterator().next(), lastLevel+1);
			
			// each calculation
			for (CalculationUnit cu : active) {
				
				// get effect of putting it in a level
				for (int level = 0; level <= lastLevel; level++) {
					int overlap = getOverlap(level, cu);
					
					// keep track of best
					if (overlap > bestOverlap) {
						bestOverlap = overlap;
						best = new Pair<CalculationUnit, Integer>(cu, level);
					}
				}
			}
			return best;
		}

		private int getOverlap(int level, CalculationUnit cu) {
			
			int overlap = 0;
			for (CalculationUnit levelledCU : levels.get(level)) {
				Set<RelationSchema> rels = levelledCU.getInputRelations();
				rels.retainAll(cu.getInputRelations());
				overlap += rels.size();
			}
			
			return overlap;
		}
		
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("Queue: ");
			sb.append(System.lineSeparator());
			sb.append("queue");
			sb.append(System.lineSeparator());
			sb.append("Levels: ");
			sb.append(System.lineSeparator());
			sb.append("levels");
			sb.append(System.lineSeparator());
			
			return sb.toString();
		}

	}

	/**
	 * @see gumbo.compiler.partitioner.CalculationPartitioner#partition(mapreduce.guardedfragment.planner.calculations.CalculationPartition)
	 */
	@Override
	public PartitionedCUGroup partition(CalculationUnitGroup partition, FileManager fm) {
		
		
		GreedyLevelQueue q = new GreedyLevelQueue(partition);
		// process all calculationunits
		while (!q.empty()) {
			
			// get all calc that have no dependencies
			Set<CalculationUnit> a = q.getActive();
			LOG.debug("Active set: " + a);
			
			// find best one
			Pair<CalculationUnit, Integer> la = q.findBestLevelAssignment(a);
			LOG.debug("Best assignment: " + la);
			
			q.level(la.fst, la.snd);
			LOG.debug(q);
			
		}
		
		PartitionedCUGroup partitionedDAG = q.createPartitionedDAG();
	
		
		
		return partitionedDAG;
	}

	

}
