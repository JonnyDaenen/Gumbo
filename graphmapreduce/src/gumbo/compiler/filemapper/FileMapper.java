/**
 * Created: 06 Feb 2015
 */
package gumbo.compiler.filemapper;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.structures.data.RelationSchema;

import org.apache.hadoop.fs.Path;

/**
 * Creates a {@link FileManager} that assigns an file to each {@link RelationSchema}. 
 * It is based on a {@link RelationFileMapping}, which must constain a mapping for each input relation.
 * Also, a scratch (or temp) dir used to store intermediate relations.
 * An output directory is also specified and indicates the location of the selected output relations.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class FileMapper {


	/**
	 * Creates a mapping from relations to locations (folders/files) on disk.
	 * All input relations are mapped to the input path, output relations are
	 * mapped to a folder in the output path, intermediate relations are mapped into a
	 * separate folder in the scratch dir.
	 * 
	 * 
	 * @pre partitionedDAG contains no overlap in input,intermediate and output
	 *      (correct implementation of getter functions :))
	 */
	public FileManager createFileMapping(RelationFileMapping rfm, Path out, Path scratch, CalculationUnitGroup dag) {

		// add input, scratch and output locations
		FileManager fm =  new FileManager(rfm, out, scratch);

		// intermediate relations
		// TODO #core this must be made more clear
		for (RelationSchema rs : dag.getIntermediateRelations()) {
			//fm.addTempRelation(rs);
			fm.addOutRelation(rs);
		}

		// output relations
		for (RelationSchema rs : dag.getOutputRelations()) {
			fm.addOutRelation(rs);
		}


		return fm;

	}




}
