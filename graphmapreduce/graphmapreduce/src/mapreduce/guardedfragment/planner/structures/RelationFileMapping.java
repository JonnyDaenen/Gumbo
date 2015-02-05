/**
 * Created: 08 Oct 2014
 */
package mapreduce.guardedfragment.planner.structures;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;

/**
 * Maps a set of relations to a specific file/folder
 * @author Jonny Daenen
 *
 */
public class RelationFileMapping {

	HashMap<RelationSchema, Set<Path>> mapping;
	private Path defaultPath;
	
	public RelationFileMapping() {
		mapping = new HashMap<>();
	}
	
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
	 * @param rs
	 * @return
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


	public Iterable<RelationSchema> getSchemas() {
		return mapping.keySet();
	}

	/**
	 * @param p
	 * @return
	 */
	public boolean containsPath(Path p) {
		for (Set<Path> paths : mapping.values()) {
			if(paths.contains(p))
				return true;
		}
		return false;
	}

	/**
	 * Sets the path to be used when no other input is available
	 * @param defaultPath
	 */
	public void setDefaultPath(Path defaultPath) {
		this.defaultPath = defaultPath;
		
	}
	
	/**
	 * @return the defaultPath
	 */
	public Path getDefaultPath() {
		return defaultPath;
	}


	
}
