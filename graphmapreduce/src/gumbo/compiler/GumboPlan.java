/**
 * Created: 06 Feb 2015
 */
package gumbo.compiler;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.partitioner.PartitionedCUGroup;
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

	String queryName = "Gumbo_query";

	// calculations
	protected PartitionedCUGroup partitions;

	// file mappings
	protected FileManager fileManager;


	public GumboPlan(String name, PartitionedCUGroup pdag, FileManager fileManager) {
		queryName = name;
		this.partitions = pdag;
		this.fileManager = fileManager;
	}

	/**
	 * @return the fileMapping
	 */
	public FileManager getFileManager() {
		return fileManager;
	}
	

	public String toString() {
		String output = "";


		// name
		output += System.getProperty("line.separator");
		output += "Query:";
		output += System.getProperty("line.separator");
		output += queryName;
		output += System.getProperty("line.separator");
		

		// calculations
//		output += System.getProperty("line.separator");
//		output += "Calculations:";
//		output += System.getProperty("line.separator");
//		output += "-------------";
//		output += System.getProperty("line.separator");
//		
//		output += calculations.toString();

		// partitions
		output += System.getProperty("line.separator");
		output += "Partitions:";
		output += System.getProperty("line.separator");
		output += "-----------";
		output += System.getProperty("line.separator");
		
		output += partitions.toString();

		// folders
		output += System.getProperty("line.separator");
		output += "Folders:";
		output += System.getProperty("line.separator");
		output += "-------";
		output += System.getProperty("line.separator");
		
		output += fileManager.toString();

		return output;

	}

	/**
	 * Returns the name of the query this plan belongs to.
	 * 
	 * @return the query name
	 */
	public String getName() {
		return queryName;
	}

	/**
	 * Returns the internal partition list
	 * @return 
	 * @return the internal partition list
	 */
	public PartitionedCUGroup getPartitions() {
		return partitions;
	}




}
