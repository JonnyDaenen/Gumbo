/**
 * Created: 08 Oct 2014
 */
package mapreduce.guardedfragment.planner.structures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.planner.structures.data.RelationSchemaException;

import org.apache.hadoop.fs.Path;

/**
 * Maps a set of relations to a specific file/folder
 * @author Jonny Daenen
 *
 */
public class RelationFileMapping {
	

	HashMap<RelationSchema, Set<Path>> mapping;
	HashMap<RelationSchema, InputFormat> format;
	private Path defaultPath;
	
	public RelationFileMapping() {
		mapping = new HashMap<>();
	}
	
	/*
	 * In the constructor RelationFileMapping,
	 * the string sIn is a string of the form: 
	 * RelSchema1 - Input File1 ; RelSchema2 - Input File2 ; .... 
	 */
	public RelationFileMapping(String sIn) throws RelationSchemaException, RelationFileMappingException {
		mapping = new HashMap<>();
		 
		String[] listIn = sIn.split(";");
		
		String[] dummy;
		
		for (int i = 0; i< listIn.length; i++) {
			dummy = listIn[i].split("-");
			
			if (dummy.length != 2) {
				throw new RelationFileMappingException("Expecting exactly one - in input file name");
			}
			
			addPath(new RelationSchema(dummy[0].trim()), new Path(dummy[1]));
		}
		
	}
	
	/**
	 * Sets the format of input files for the given relation schema.
	 * @param s the relation schema
	 * @param f the format
	 */
	public void setFormat(RelationSchema s, InputFormat f) {
		format.put(s, f);
	}
	
	/**
	 * Fetches the format of the input files for the given relationschema.
	 * @param s the relationschema
	 * @return the file format of the input files
	 */
	public InputFormat getFormat(RelationSchema s) {
		if (format.containsKey(s))
			return format.get(s);
		return InputFormat.REL;
	}
	
	/**
	 * Adds a path to the set of input paths of the relation schema.
	 * This can be a file or directory.
	 * @param s the relationschema
	 * @param p an input path for the relation schema
	 */
	public void addPath(RelationSchema s, Path p) {
		Set<Path> paths;
		if(mapping.containsKey(s)) {
			paths = mapping.get(s);
		} else {
			paths = new HashSet<Path>();
			mapping.put(s, paths);
		}
		paths.add(p);
	}

	/**
	 * @param infiles the mapping to incorporate into this one
	 * @param copyDefault true iff the default path should be copied too
	 */
	public void putAll(RelationFileMapping infiles, boolean copyDefault) {
		for (RelationSchema rs : infiles.mapping.keySet()) {
			Set<Path> set = infiles.mapping.get(rs);
			for (Path path : set) {
				addPath(rs,path);
			}
		}
		
		if (copyDefault) {
			defaultPath = infiles.defaultPath;
		}
		
	}




	/**
	 * Checks if there is a file mapping present for a schema.
	 * @param rs a relation schema
	 * @return true iff there is a set of paths present
	 */
	public boolean containsSchema(RelationSchema rs) {
		return mapping.containsKey(rs);
	}

	/**
	 * Gets the set of paths associated with a relationschema.
	 * @param rs a relation schema
	 * @return the pats of the relationschema, null if
	 */
	public Set<Path> getPaths(RelationSchema rs) {
		if (!containsSchema(rs))
			return new HashSet<>();
		return mapping.get(rs);
	}


	/**
	 * Fetches all the relation schemas in the mapping.
	 * @return all the relation schemas in the mapping
	 */
	public Iterable<RelationSchema> getSchemas() {
		return mapping.keySet();
	}

	/**
	 * Checks if a given path is coupled to a relation schema
	 * @param p a path
	 * @return true iff there is a relation schema with this path
	 */
	public boolean containsPath(Path p) {
		for (Set<Path> paths : mapping.values()) {
			if(paths.contains(p))
				return true;
		}
		return false;
	}

	/**
	 * Sets the path to be used when no other input is available.
	 * @param defaultPath a path
	 */
	public void setDefaultPath(Path defaultPath) {
		this.defaultPath = defaultPath;
		
	}
	
	/**
	 * Fetches the default path to be used when no other input is available.
	 * @return the default path
	 */
	public Path getDefaultPath() {
		return defaultPath;
	}


	
}
