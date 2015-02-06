/**
 * Created: 06 Feb 2015
 */
package gumbo.compiler;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCalculationUnitGroup;
import gumbo.compiler.structures.data.RelationSchema;

/**
 * A Gumbo query plan.
 * Contains a DAG of {@link CalculationGroup}s, and a mapping of {@link RelationSchema}s
 * to locations in the file system.
 * 
 * @author Jonny Daenen
 *
 */
public class GumboPlan {
	
	String queryName = "Gumbo_v1";
	
	// calculations
	protected CalculationUnitGroup calculations;
	protected PartitionedCalculationUnitGroup partitions;
	
	// file mappings
	protected RelationFileMapping fileMapping;
	protected Path outputPath;
	protected Path scratchPath;
	
	
	/* getters & setters */
	
	public RelationFileMapping getFileMapping() {
		return fileMapping;
	}
	public void setFilemapping(RelationFileMapping fileMapping) {
		this.fileMapping = fileMapping;
	}
	public Path getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}
	public Path getScratchPath() {
		return scratchPath;
	}
	public void setScratchPath(Path scratchPath) {
		this.scratchPath = scratchPath;
	}
	

}
