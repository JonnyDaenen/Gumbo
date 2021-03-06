/**
 * Created: 29 Apr 2014
 */
package gumbo.compiler.partitioner;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.linker.CalculationUnitGroup;

/**
 * Divides a set of CalculationUnits into a a sequence of partitions,
 * based on their dependencies
 * 
 * @author Jonny Daenen
 *
 */
public interface CalculationPartitioner {


	PartitionedCUGroup partition(CalculationUnitGroup calculations, FileManager fm);

}
