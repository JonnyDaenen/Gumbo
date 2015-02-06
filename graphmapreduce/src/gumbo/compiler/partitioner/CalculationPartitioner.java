/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.resolver.DirManager;

/**
 * Divides a set of CalculationUnits into a a sequence of partitions,
 * based on their dependencies
 * 
 * @author Jonny Daenen
 *
 */
public interface CalculationPartitioner {


	PartitionedCalculationUnitGroup partition(CalculationUnitGroup calculations, DirManager dm);

}
