/**
 * Created on: 24 Feb 2015
 */
package gumbo.engine.spark;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.PartitionQueue;

import java.util.Collection;

/**
 * @author Jonny Daenen
 *
 */
public class SparkPartitionQueue extends PartitionQueue {
	

	Collection<CalculationUnitGroup> finishedCUGs;
	
	public SparkPartitionQueue(PartitionedCUGroup pcug) {
		super(pcug);
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

}