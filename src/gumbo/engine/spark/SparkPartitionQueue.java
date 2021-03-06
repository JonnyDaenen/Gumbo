/**
 * Created on: 24 Feb 2015
 */
package gumbo.engine.spark;

import java.util.Collection;
import java.util.HashSet;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.utils.PartitionQueue;

/**
 * @author Jonny Daenen
 *
 */
public class SparkPartitionQueue extends PartitionQueue {
	

	Collection<CalculationUnitGroup> finishedCUGs;
	
	public SparkPartitionQueue(PartitionedCUGroup pcug) {
		super(pcug);
		finishedCUGs = new HashSet<>();
	}

	/* (non-Javadoc)
	 * @see gumbo.engine.general.PartitionQueue#isReady(gumbo.compiler.linker.CalculationUnitGroup)
	 */
	@Override
	protected boolean isReady(CalculationUnitGroup group) {
		return finishedCUGs.contains(group);
	}
	
	
	public void setFinished(CalculationUnitGroup group) {
		finishedCUGs.add(group);
	}

	@Override
	protected void cleanup(CalculationUnitGroup group) {
		// TODO Auto-generated method stub
		
	}

}
